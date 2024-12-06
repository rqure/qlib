SERVER_INTERACTOR_CONNECTION_STATES = {
    DISCONNECTED: 0,
    CONNECTING: 1,
    CONNECTED: 2,
    DISCONNECTING: 3
};

class ServerInteractor {
    constructor(url) {
        this._url = url;
        this._ws = null;
        this._connectionStatus = SERVER_INTERACTOR_CONNECTION_STATES.DISCONNECTED;
        this._waitingResponses = {};
    }

    isConnected() {
        return this._connectionStatus === SERVER_INTERACTOR_CONNECTION_STATES.CONNECTED;
    }

    onMessage(event) {
        const me = this;
        const fileReader = new FileReader();

        fileReader.onload = function(event) {
            const message = proto.qdb.WebMessage.deserializeBinary(new Uint8Array(event.target.result));
            const requestId = message.getHeader().getId();

            if (!me._waitingResponses[requestId]) {
                qWarn("[ServerInteractor::onMessage] Received response for unknown request '" + requestId + "'");
                return;
            }

            const request = me._waitingResponses[requestId];
            const response = request.responseType.deserializeBinary(message.getPayload().getValue_asU8());
            if (response) {
                request.resolve(response);
            } else {
                request.reject(new Error('Invalid response'));
            }
        }
        fileReader.readAsArrayBuffer(event.data);
    }

    onOpen(event) {
        qInfo("[ServerInteractor::onOpen] Connection established with '" + this._url + "'");
        this._connectionStatus = SERVER_INTERACTOR_CONNECTION_STATES.CONNECTED;
    }

    onClose(event) {
        qWarn("[ServerInteractor::onClose] Connection closed with '" + this._url + "'");
        
        if( this._ws ) {
            this._ws.removeEventListener('open', this.onOpen.bind(this));
            this._ws.removeEventListener('message', this.onMessage.bind(this));
            this._ws.removeEventListener('close', this.onClose.bind(this));
            this._ws = null;
        }
        
        this._connectionStatus = SERVER_INTERACTOR_CONNECTION_STATES.DISCONNECTED;

        for (const requestId in this._waitingResponses) {
            const request = this._waitingResponses[requestId];
            request.reject(new Error('Connection closed'));
        }
        
        this._waitingResponses = {};
    }

    connect() {
        if (this._connectionStatus !== SERVER_INTERACTOR_CONNECTION_STATES.DISCONNECTED) {
            qError("[ServerInteractor::connect] Connection already exists, disconnecting first.");
            this.disconnect();
            return;
        }
        
        qInfo("[ServerInteractor::connect] Connecting to '" + this._url + "'")
        this._connectionStatus = SERVER_INTERACTOR_CONNECTION_STATES.CONNECTING;
        this._ws = new WebSocket(this._url);
        
        this._ws.addEventListener('open', this.onOpen.bind(this));
        this._ws.addEventListener('message', this.onMessage.bind(this));
        this._ws.addEventListener('close', this.onClose.bind(this));
    }

    disconnect() {
        if (this._ws) {
            qInfo("[ServerInteractor::disconnect] Disconnecting from '" + this._url + "'")

            this._connectionStatus = SERVER_INTERACTOR_CONNECTION_STATES.DISCONNECTING;

            this._ws.close();
        } else {
            this._connectionStatus = SERVER_INTERACTOR_CONNECTION_STATES.DISCONNECTED;
        }
    }

    async send(requestProto, responseProtoType) {
        const requestId = uuidv4();
        const request = this._waitingResponses[requestId] = { "sent": +new Date(), "responseType": responseProtoType };

        const header = new proto.qdb.WebHeader();
        header.setId(requestId);
        header.setTimestamp(new proto.google.protobuf.Timestamp.fromDate(new Date()));

        const message = new proto.qdb.WebMessage();
        message.setHeader(header);
        message.setPayload(new proto.google.protobuf.Any());
        message.getPayload().pack(requestProto.serializeBinary(), qMessageType(requestProto));

        try {
            if (this.isConnected()) {
                this._ws.send(message.serializeBinary());
            }

            qTrace("[ServerInteractor::send] Request '" + requestId + "' sent");

            const result = await new Promise((resolve, reject) => {
                request.resolve = resolve;
                request.reject = reject;

                if (!this.isConnected()) {
                    reject(new Error('Connection closed'));
                }
            });

            qTrace("[ServerInteractor::send] Response for '" + requestId + "' received in " + (new Date() - request.sent) + "ms");

            return result;
        } finally {
            delete this._waitingResponses[requestId];
        }
    }
}
