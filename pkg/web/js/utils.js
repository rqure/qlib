function uuidv4() {
    return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
      (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
    );
}

const Q_LOG_LEVELS = {
    TRACE: 0,
    DEBUG: 1,
    INFO: 2,
    WARN: 3,
    ERROR: 4,
    PANIC: 5,
}

let Q_CURRENT_LOG_LEVEL = Q_LOG_LEVELS.TRACE;

function qLog(level, message) {
    if (level < Q_CURRENT_LOG_LEVEL) {
        return;
    }

    console.log(`${new Date().toISOString()} | ${Object.keys(Q_LOG_LEVELS).find(key => Q_LOG_LEVELS[key] === level)} | ${message}`);
}

function qTrace(message) {
    qLog(Q_LOG_LEVELS.TRACE, message);
}

function qDebug(message) {
    qLog(Q_LOG_LEVELS.DEBUG, message);
}

function qInfo(message) {
    qLog(Q_LOG_LEVELS.INFO, message);
}

function qWarn(message) {
    qLog(Q_LOG_LEVELS.WARN, message);
}

function qError(message) {
    qLog(Q_LOG_LEVELS.ERROR, message);
}

function qPanic(message) {
    qLog(Q_LOG_LEVELS.PANIC, message);
}

function qMessageType(message) {
    for (const key in proto.db) {
        if (message instanceof proto.db[key]) {
            return "db." + key
        }
    }

    return null
}
