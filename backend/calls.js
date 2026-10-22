"use strict";

/**
 * calls.js — Règles métier des appels (salons), sans aucune dépendance réseau.
 *
 * Principe : tout appel est un SALON (même à 2). Le média est en mesh à
 * l'intérieur du salon ; le serveur ne fait que tenir l'état des participants
 * et relayer la signalisation (offer / answer / ICE) entre deux participants.
 *
 * Règle anti-doublons : pour l'offer INITIALE d'une paire, c'est toujours celui
 * qui est arrivé le plus tard (seq le plus grand) qui l'envoie. Le serveur
 * attribue `seq` et refuse une offer initiale dans le mauvais sens.
 *
 * Ce module reçoit un objet `io` pour parler au monde extérieur :
 *   io.isOnline(userId)  -> boolean   (WebSocket ouvert ?)
 *   io.hasPush(userId)   -> boolean   (token FCM connu ?)
 *   io.send(userId, msg) -> boolean   (envoi WebSocket)
 *   io.push(userId, kind, { call, from })   kind = "incoming" | "missed"
 */

const crypto = require("crypto");

const int = (v, d) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : d;
};
const list = (v, d = []) =>
  v
    ? String(v)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : d;

const DEFAULTS = {
  // Limites (comptées sur les participants actifs : en sonnerie + dans l'appel)
  MAX_AUDIO_PARTICIPANTS: int(process.env.MAX_AUDIO_PARTICIPANTS, 8),
  MAX_VIDEO_PARTICIPANTS: int(process.env.MAX_VIDEO_PARTICIPANTS, 4),
  // Délais
  RING_TIMEOUT_MS: int(process.env.RING_TIMEOUT_MS, 45000),
  RECONNECT_GRACE_MS: int(process.env.RECONNECT_GRACE_MS, 15000),
  // ICE
  STUN_URLS: list(process.env.STUN_URLS, ["stun:stun.l.google.com:19302"]),
  TURN_URLS: list(process.env.TURN_URLS), // ex: turn:turn.mondomaine.com:3478,turns:turn.mondomaine.com:5349
  TURN_SECRET: process.env.TURN_SECRET || "", // même valeur que static-auth-secret de coturn
  TURN_TTL_SECONDS: int(process.env.TURN_TTL_SECONDS, 6 * 3600),
};

class CallError extends Error {
  constructor(code, message) {
    super(message || code);
    this.code = code;
  }
}

const ACTIVE = new Set(["RINGING", "JOINED"]);
const randomId = () => crypto.randomBytes(8).toString("hex");

class CallManager {
  constructor(io, options = {}) {
    this.io = io;
    this.cfg = { ...DEFAULTS, ...options };
    this.calls = new Map(); // callId -> call
    this.userCall = new Map(); // userId -> callId (uniquement s'il est JOINED)
    this.graceTimers = new Map(); // userId -> timer (coupure WebSocket en cours d'appel)
  }

  /* ---------------------------------------------------------
     Création / invitation
  --------------------------------------------------------- */

  createCall(hostId, targetIds, { isVideo = false, notId } = {}) {
    if (this.userCall.has(hostId)) throw new CallError("already-in-call");
    const targets = [...new Set(Array.isArray(targetIds) ? targetIds : [])].filter(
      (t) => typeof t === "string" && t && t !== hostId,
    );
    if (!targets.length) throw new CallError("no-target");

    isVideo = !!isVideo;
    if (1 + targets.length > this._max(isVideo)) throw this._limitError(isVideo);

    const call = {
      id: `call_${Date.now()}_${randomId()}`,
      host: hostId,
      isVideo,
      notId: notId || Math.floor(100000 + Math.random() * 900000),
      seq: 0,
      answered: false, // passe à true dès que 2 personnes ont été JOINED ensemble
      ended: false,
      participants: new Map(),
    };
    this.calls.set(call.id, call);
    this._markJoined(call, hostId);

    const invited = [];
    const unreachable = [];
    for (const t of targets) {
      const r = this._ring(call, t);
      (r.ok ? invited : unreachable).push(r.entry);
    }

    if (!invited.length) {
      this._destroy(call);
      return { callId: null, invited: [], unreachable };
    }
    // Livraison après l'enregistrement de tous les invités (isGroup doit être exact)
    this._deliverInvites(call, invited);
    return {
      callId: call.id,
      snapshot: this.snapshot(call, hostId),
      invited,
      unreachable,
    };
  }

  invite(userId, callId, targetIds) {
    const call = this._get(callId);
    this._requireJoined(call, userId);

    const targets = [...new Set(Array.isArray(targetIds) ? targetIds : [])].filter(
      (t) => typeof t === "string" && t && t !== userId,
    );
    const unreachable = [];
    const toRing = [];
    for (const t of targets) {
      const p = call.participants.get(t);
      if (p && ACTIVE.has(p.state)) unreachable.push({ userId: t, reason: "already-in-call" });
      else toRing.push(t);
    }
    if (!toRing.length) return { invited: [], unreachable };

    if (this._activeCount(call) + toRing.length > this._max(call.isVideo)) {
      throw this._limitError(call.isVideo);
    }

    const invited = [];
    for (const t of toRing) {
      const r = this._ring(call, t);
      if (r.ok) invited.push(r.entry);
      else unreachable.push(r.entry);
    }
    this._deliverInvites(call, invited);
    for (const e of invited) {
      this._broadcast(call, { type: "participant-ringing", callId, userId: e.userId });
    }
    return { invited, unreachable };
  }

  /* ---------------------------------------------------------
     Rejoindre / refuser / quitter
  --------------------------------------------------------- */

  join(userId, callId, { fresh = false } = {}) {
    const call = this._get(callId);
    const p = call.participants.get(userId);
    if (!p) throw new CallError("not-invited");

    const other = this.userCall.get(userId);
    if (other && other !== callId) throw new CallError("already-in-call");

    // Déjà dans l'appel : reconnexion de la signalisation
    if (p.state === "JOINED") {
      this._clearGrace(userId);
      if (fresh) {
        // Le client a perdu ses connexions média : nouvelle session, les autres
        // remplacent leur connexion avec lui.
        p.seq = ++call.seq;
        p.sessionId = randomId();
        this._broadcast(call, this._joinedMsg(call, p), userId);
      }
      return this.snapshot(call, userId);
    }

    if (this._activeCount(call, userId) + 1 > this._max(call.isVideo)) {
      throw this._limitError(call.isVideo);
    }

    this._markJoined(call, userId);
    this._clearGrace(userId);
    this._broadcast(call, this._joinedMsg(call, p), userId);
    return this.snapshot(call, userId);
  }

  decline(userId, callId) {
    const call = this._get(callId);
    const p = call.participants.get(userId);
    if (!p) throw new CallError("not-invited");
    if (p.state === "JOINED") return this.leave(userId, callId);
    if (p.state !== "RINGING") return;

    clearTimeout(p.ringTimer);
    p.ringTimer = null;
    p.state = "REFUSED";
    this._broadcast(call, { type: "participant-declined", callId, userId });
    this._evaluate(call);
  }

  leave(userId, callId, { reason = "left" } = {}) {
    const call = this.calls.get(callId);
    if (!call || call.ended) return;
    const p = call.participants.get(userId);
    if (!p) return;
    if (p.state === "RINGING") return this.decline(userId, callId);
    if (p.state !== "JOINED") return;

    this._clearGrace(userId);
    p.state = "LEFT";
    this.userCall.delete(userId);
    this._broadcast(call, {
      type: "participant-left",
      callId,
      userId,
      seq: p.seq,
      sessionId: p.sessionId,
      reason,
    });
    if (call.host === userId) this._reassignHost(call);
    this._evaluate(call);
  }

  /* ---------------------------------------------------------
     Vidéo et état média
  --------------------------------------------------------- */

  upgradeToVideo(userId, callId) {
    const call = this._get(callId);
    this._requireJoined(call, userId);
    if (call.isVideo) return;
    if (this._activeCount(call) > this.cfg.MAX_VIDEO_PARTICIPANTS) {
      throw this._limitError(true);
    }
    call.isVideo = true;
    this._broadcast(call, { type: "call-upgraded", callId, isVideo: true, by: userId });
  }

  setMediaState(userId, callId, { audio, video } = {}) {
    const call = this._get(callId);
    const p = this._requireJoined(call, userId);
    if (video === true && !call.isVideo) throw new CallError("not-a-video-call");
    if (typeof audio === "boolean") p.audio = audio;
    if (typeof video === "boolean") p.video = video;
    this._broadcast(
      call,
      { type: "participant-media", callId, userId, audio: p.audio, video: p.video },
      userId,
    );
  }

  /* ---------------------------------------------------------
     Relais de signalisation (offer / answer / ICE / restart-offer)
  --------------------------------------------------------- */

  relay(fromId, type, data) {
    const { callId, to, toSession } = data;
    const call = this._get(callId);
    const from = this._requireJoined(call, fromId);
    const target = call.participants.get(to);

    // Cible partie entre-temps : cas normal (candidats ICE en retard), pas une erreur.
    if (!target || target.state !== "JOINED") return { delivered: false, reason: "target-not-joined" };
    // Message destiné à une ancienne session de la cible : on l'ignore.
    if (toSession && toSession !== target.sessionId) return { delivered: false, reason: "stale-session" };

    // Règle anti-doublons : l'offer initiale part du dernier arrivé vers le plus ancien.
    if (type === "offer" && from.seq < target.seq) {
      throw new CallError(
        "bad-offer-direction",
        "L'offer initiale doit être envoyée par le participant arrivé le plus tard",
      );
    }

    const envelope = { type, callId, from: fromId, fromSession: from.sessionId };
    for (const k of ["offer", "answer", "candidate"]) {
      if (data[k] !== undefined) envelope[k] = data[k];
    }
    const delivered = this.io.send(to, envelope);
    return { delivered };
  }

  /* ---------------------------------------------------------
     Connexion / déconnexion de la signalisation
  --------------------------------------------------------- */

  onUserDisconnected(userId) {
    const callId = this.userCall.get(userId);
    if (!callId) return;
    this._clearGrace(userId);
    const timer = setTimeout(() => {
      this.graceTimers.delete(userId);
      this.leave(userId, callId, { reason: "connection-lost" });
    }, this.cfg.RECONNECT_GRACE_MS);
    timer.unref?.();
    this.graceTimers.set(userId, timer);
  }

  onUserReconnected(userId) {
    this._clearGrace(userId);
  }

  /* ---------------------------------------------------------
     Consultation
  --------------------------------------------------------- */

  activeCallOf(userId) {
    return this.userCall.get(userId) || null;
  }

  ringingFor(userId) {
    const out = [];
    for (const call of this.calls.values()) {
      const p = call.participants.get(userId);
      if (!call.ended && p && p.state === "RINGING") out.push(this._info(call));
    }
    return out;
  }

  getCallInfo(userId, callId) {
    const call = this.calls.get(callId);
    const p = call?.participants.get(userId);
    if (!call || call.ended || !p || !ACTIVE.has(p.state)) return null;
    return { ...this._info(call), state: p.state };
  }

  isGroup(call) {
    return this._activeCount(call) > 2;
  }

  snapshot(call, forUserId) {
    const me = forUserId ? call.participants.get(forUserId) : null;
    return {
      callId: call.id,
      isVideo: call.isVideo,
      host: call.host,
      notId: call.notId,
      you: me ? { userId: me.userId, seq: me.seq, sessionId: me.sessionId } : undefined,
      participants: [...call.participants.values()].map((p) => ({
        userId: p.userId,
        state: p.state,
        seq: p.seq,
        sessionId: p.sessionId,
        audio: p.audio,
        video: p.video,
      })),
      iceServers: forUserId ? this.buildIceServers(forUserId) : undefined,
    };
  }

  /** STUN + TURN avec identifiants éphémères (coturn : use-auth-secret). */
  buildIceServers(userId) {
    const servers = [{ urls: this.cfg.STUN_URLS }];
    if (this.cfg.TURN_URLS.length && this.cfg.TURN_SECRET) {
      const expiry = Math.floor(Date.now() / 1000) + this.cfg.TURN_TTL_SECONDS;
      const username = `${expiry}:${userId}`;
      const credential = crypto
        .createHmac("sha1", this.cfg.TURN_SECRET)
        .update(username)
        .digest("base64");
      servers.push({ urls: this.cfg.TURN_URLS, username, credential });
    }
    return servers;
  }

  /* ---------------------------------------------------------
     Interne
  --------------------------------------------------------- */

  _max(isVideo) {
    return isVideo ? this.cfg.MAX_VIDEO_PARTICIPANTS : this.cfg.MAX_AUDIO_PARTICIPANTS;
  }

  _limitError(isVideo) {
    return isVideo
      ? new CallError("video-limit", `Un appel vidéo est limité à ${this.cfg.MAX_VIDEO_PARTICIPANTS} participants`)
      : new CallError("participant-limit", `Un appel est limité à ${this.cfg.MAX_AUDIO_PARTICIPANTS} participants`);
  }

  _get(callId) {
    const call = this.calls.get(callId);
    if (!call || call.ended) throw new CallError("call-not-found");
    return call;
  }

  _requireJoined(call, userId) {
    const p = call.participants.get(userId);
    if (!p || p.state !== "JOINED") throw new CallError("not-in-call");
    return p;
  }

  _byState(call, state) {
    return [...call.participants.values()].filter((p) => p.state === state);
  }

  _activeCount(call, exceptUserId) {
    let n = 0;
    for (const p of call.participants.values()) {
      if (p.userId !== exceptUserId && ACTIVE.has(p.state)) n++;
    }
    return n;
  }

  _info(call) {
    return {
      callId: call.id,
      from: call.host,
      isVideo: call.isVideo,
      isGroup: this.isGroup(call),
      notId: call.notId,
    };
  }

  _joinedMsg(call, p) {
    return {
      type: "participant-joined",
      callId: call.id,
      userId: p.userId,
      seq: p.seq,
      sessionId: p.sessionId,
    };
  }

  _broadcast(call, msg, exceptUserId) {
    for (const p of call.participants.values()) {
      if (p.state === "JOINED" && p.userId !== exceptUserId) this.io.send(p.userId, msg);
    }
  }

  _markJoined(call, userId) {
    let p = call.participants.get(userId);
    if (!p) {
      p = { userId, state: "RINGING", seq: null, sessionId: null, audio: true, video: false, ringTimer: null };
      call.participants.set(userId, p);
    }
    clearTimeout(p.ringTimer);
    p.ringTimer = null;
    p.state = "JOINED";
    p.seq = ++call.seq;
    p.sessionId = randomId();
    p.audio = true;
    p.video = false;
    this.userCall.set(userId, call.id);
    if (this._byState(call, "JOINED").length >= 2) call.answered = true;
    return p;
  }

  _ring(call, userId) {
    if (this.userCall.has(userId)) return { ok: false, entry: { userId, reason: "busy" } };
    const online = this.io.isOnline(userId);
    const push = this.io.hasPush(userId);
    if (!online && !push) return { ok: false, entry: { userId, reason: "offline" } };

    let p = call.participants.get(userId);
    if (!p) {
      p = { userId, state: "RINGING", seq: null, sessionId: null, audio: true, video: false, ringTimer: null };
      call.participants.set(userId, p);
    } else {
      clearTimeout(p.ringTimer);
      p.state = "RINGING";
      p.seq = null;
      p.sessionId = null;
    }
    p.ringTimer = setTimeout(() => this._onRingTimeout(call.id, userId), this.cfg.RING_TIMEOUT_MS);
    p.ringTimer.unref?.();

    return { ok: true, entry: { userId, via: online ? (push ? "ws+push" : "ws") : "push" } };
  }

  _deliverInvites(call, invitedEntries) {
    const info = this._info(call);
    for (const { userId, via } of invitedEntries) {
      if (via !== "push") this.io.send(userId, { type: "incoming-call", ...info });
      if (via !== "ws") this.io.push(userId, "incoming", { call, from: call.host });
    }
  }

  _onRingTimeout(callId, userId) {
    const call = this.calls.get(callId);
    const p = call?.participants.get(userId);
    if (!call || call.ended || !p || p.state !== "RINGING") return;

    p.state = "MISSED";
    p.ringTimer = null;
    this.io.send(userId, { type: "call-cancelled", callId, reason: "timeout" });
    if (this.io.hasPush(userId)) this.io.push(userId, "missed", { call, from: call.host });
    this._broadcast(call, { type: "participant-missed", callId, userId });
    this._evaluate(call);
  }

  _reassignHost(call) {
    const joined = this._byState(call, "JOINED").sort((a, b) => a.seq - b.seq);
    if (!joined.length) return;
    call.host = joined[0].userId;
    this._broadcast(call, { type: "host-changed", callId: call.id, host: call.host });
  }

  /**
   * Règle de fin : un appel continue tant qu'au moins 2 participants sont dans
   * l'appel, ou qu'il reste quelqu'un à qui l'appel sonne encore.
   */
  _evaluate(call) {
    if (call.ended) return;
    const joined = this._byState(call, "JOINED");
    const ringing = this._byState(call, "RINGING");

    if (joined.length === 0) {
      this._end(call, call.answered ? "ended" : "no-answer");
      return;
    }
    if (joined.length === 1 && ringing.length === 0) {
      let reason = "ended";
      if (!call.answered) {
        const others = [...call.participants.values()].filter((p) => p.userId !== joined[0].userId);
        reason = others.length && others.every((p) => p.state === "REFUSED") ? "refused" : "no-answer";
      }
      this._end(call, reason);
    }
  }

  _end(call, reason) {
    if (call.ended) return;
    call.ended = true;
    for (const p of call.participants.values()) {
      clearTimeout(p.ringTimer);
      p.ringTimer = null;
      if (p.state === "JOINED") {
        this.userCall.delete(p.userId);
        this._clearGrace(p.userId);
        this.io.send(p.userId, { type: "call-ended", callId: call.id, reason });
      } else if (p.state === "RINGING") {
        // Appel annulé avant d'avoir été décroché : appel manqué
        p.state = "MISSED";
        this.io.send(p.userId, { type: "call-cancelled", callId: call.id, reason: "caller-ended" });
        if (this.io.hasPush(p.userId)) this.io.push(p.userId, "missed", { call, from: call.host });
      }
    }
    this.calls.delete(call.id);
  }

  _destroy(call) {
    call.ended = true;
    for (const p of call.participants.values()) {
      clearTimeout(p.ringTimer);
      if (p.state === "JOINED") this.userCall.delete(p.userId);
    }
    this.calls.delete(call.id);
  }

  _clearGrace(userId) {
    const t = this.graceTimers.get(userId);
    if (t) {
      clearTimeout(t);
      this.graceTimers.delete(userId);
    }
  }
}

module.exports = { CallManager, CallError, DEFAULTS };
