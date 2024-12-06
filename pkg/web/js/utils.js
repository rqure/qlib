function uuidv4() {
    return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, c =>
      (+c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> +c / 4).toString(16)
    );
}

const LOG_LEVELS = {
    TRACE: 0,
    DEBUG: 1,
    INFO: 2,
    WARN: 3,
    ERROR: 4,
    PANIC: 5,
}

let CURRENT_LOG_LEVEL = LOG_LEVELS.TRACE;

function qLog(level, message) {
    if (level < CURRENT_LOG_LEVEL) {
        return;
    }

    console.log(`${new Date().toISOString()} | ${Object.keys(LOG_LEVELS).find(key => LOG_LEVELS[key] === level)} | ${message}`);
}

function qTrace(message) {
    qLog(LOG_LEVELS.TRACE, message);
}

function qDebug(message) {
    qLog(LOG_LEVELS.DEBUG, message);
}

function qInfo(message) {
    qLog(LOG_LEVELS.INFO, message);
}

function qWarn(message) {
    qLog(LOG_LEVELS.WARN, message);
}

function qError(message) {
    qLog(LOG_LEVELS.ERROR, message);
}

function qPanic(message) {
    qLog(LOG_LEVELS.PANIC, message);
}

function qMessageType(message) {
    for (const key in proto.qdb) {
        if (message instanceof proto.qdb[key]) {
            return "qdb." + key
        }
    }

    return null
}
