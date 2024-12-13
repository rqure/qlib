Q_STORE_EVENTS = {
    CONNECTED: "connected",
    DISCONNECTED: "disconnected",
};

class QNotificationListener {
    constructor(eventName, callback) {
        this._eventName = eventName;
        this._callback = callback;
    }

    getEventName() {
        return this._eventName;
    }

    getCallback() {
        return this._callback;
    }

    invokeCallback(eventData) {
        this._callback(eventData);
    }
}

class QNotificationManager {
    constructor() {
        this._listeners = {};
    }

    addEventListener(eventName, callback) {
        if (!this._listeners[eventName]) {
            this._listeners[eventName] = [];
        }

        this._listeners[eventName].push(new QNotificationListener(eventName, callback));

        return this;
    }

    removeEventListener(eventName, callback) {
        if (!this._listeners[eventName]) {
            return;
        }

        this._listeners[eventName] = this._listeners[eventName].filter(listener => listener.callback !== callback);
    }

    dispatchEvent(eventName, eventData) {
        if (!this._listeners[eventName]) {
            return;
        }

        this._listeners[eventName].forEach(listener => listener.invokeCallback(eventData));
    }

}

class QEntityStore {
    constructor(overrides) {
        let port = location.port == "" ? "" : ":" + location.port;
        this._mainLoopInterval = 500;
        this._connectionBackoffTime = 2500;
        this._tokens = [];

        if (overrides) {
            if ("port" in overrides) {
                port = overrides.port;
            }
            if ("mainLoopInterval" in overrides) {
                this._mainLoopInterval = overrides.mainLoopInterval;
            }
            if ("connectionBackoffTime" in overrides) {
                this._connectionBackoffTime = overrides.connectionBackoffTime;
            }
        }
        this._server = new QServer(`${location.protocol == "https:" ? "wss:" : "ws:"}//${location.hostname}${port}/ws`);
        this._notificationManager = new QNotificationManager();
        this._runInBackground = false;
        this._isConnected = null;
        this._lastConnnectionAttempt = new Date(0).getTime();
    }

    isConnected() {
        return this._isConnected;
    }

    getServer() {
        return this._server;
    }

    getEventManager() {
        return this._notificationManager;
    }

    getAvailableFieldTypes() {
        return Object.keys(proto.protobufs).filter(type => !type.startsWith("Web") && !type.startsWith("Database"));
    }

    setMainLoopInterval(interval) {
        this._mainLoopInterval = interval
    }

    runInBackground(runInBackground) {
        this._runInBackground = runInBackground;

        this.mainLoop();
    }

    mainLoop() {
        if (!this._runInBackground) {
            return;
        }

        if (!this._server.isConnected()) {
            if (this._isConnected !== false) {
                this._isConnected = false;
                this._notificationManager.dispatchEvent(Q_STORE_EVENTS.DISCONNECTED, {});
            }

            const currentTime = new Date().getTime();
            if ((this._lastConnnectionAttempt + this._connectionBackoffTime) <= currentTime) {
                this._server.connect();
                this._lastConnnectionAttempt = currentTime;
            }

            setTimeout(() => {
                this.mainLoop();
            }, this._mainLoopInterval);

            return;
        }

        this._server
            .send(new proto.protobufs.WebRuntimeGetDatabaseConnectionStatusRequest(), proto.protobufs.WebRuntimeGetDatabaseConnectionStatusResponse)
            .then(response => {
                if (!response.getConnected()) {
                    if(this._isConnected !== false) {
                        this._isConnected = false;
                        this._notificationManager.dispatchEvent(Q_STORE_EVENTS.DISCONNECTED, {});
                    }
                } else {
                    if(this._isConnected !== true) {
                        this._isConnected = true;

                        /**
                         * Unregister all notifications when the connection is re-established
                         * This is to prevent duplicate notifications from being registered
                         */
                        this.unregisterNotifications(this._tokens);

                        this._notificationManager.dispatchEvent(Q_STORE_EVENTS.CONNECTED, {});
                    }
                }
            })
            .catch(error => {
                qError(`[QEntityStore::mainLoop] Failed to get database connection status: ${error}`);
            });
        
        this.processNotifications();

        setTimeout(() => {
            this.mainLoop();
        }, this._mainLoopInterval);
    }

    createEntity(parentId, entityName, entityType) {
        const me = this;
        const request = new proto.protobufs.WebConfigCreateEntityRequest();
        request.setParentid(parentId);
        request.setName(entityName);
        request.setType(entityType);

        return me._server
            .send(request, proto.protobufs.WebConfigCreateEntityResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigCreateEntityResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::createEntity] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {entityName: entityName, entityType: entityType, parentId: parentId};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::createEntity] Failed to create entity: ${error}`);
            });
    }

    queryAllEntities(entityType) {
        const request = new proto.protobufs.WebRuntimeGetEntitiesRequest();
        request.setEntitytype(entityType);

        return this._server
            .send(request, proto.protobufs.WebRuntimeGetEntitiesResponse)
            .then(response => {
                return {entities: response.getEntitiesList()};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::queryAllEntities] Failed to get all entities: ${error}`);
            });
    }

    queryEntity(entityId) {
        const request = new proto.protobufs.WebConfigGetEntityRequest();
        request.setId(entityId);
        
        return this._server
            .send(request, proto.protobufs.WebConfigGetEntityResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigGetEntityResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::queryEntity] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {entity: response.getEntity()};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::queryEntity] Failed to get entity: ${error}`);
            });
    }

    queryEntitySchema(entityType) {
        const request = new proto.protobufs.WebConfigGetEntitySchemaRequest();
        request.setType(entityType);

        return this._server
            .send(request, proto.protobufs.WebConfigGetEntitySchemaResponse)
            .then(response => {
                if(response.getStatus() !== proto.protobufs.WebConfigGetEntitySchemaResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::queryEntitySchema] Could not complete the request: ${response.getStatus()}`);
                }

                return {schema: response.getSchema()};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::queryEntitySchema] Failed to get entity schema: ${error}`);
            });
    }

    queryAllEntityTypes() {
        return this._server
            .send(new proto.protobufs.WebConfigGetEntityTypesRequest(), proto.protobufs.WebConfigGetEntityTypesResponse)
            .then(response => {
                return {entityTypes: response.getTypesList()};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::queryAllEntityTypes] Failed to get all entity types: ${error}`);
            });
    }

    deleteEntity(entityId) {
        const me = this;
        const request = new proto.protobufs.WebConfigDeleteEntityRequest();
        request.setId(entityId);

        return me._server
            .send(request, proto.protobufs.WebConfigDeleteEntityResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigDeleteEntityResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::deleteEntity] Could not complete the request: ${response.getStatus()}`);
                }

                return {entityId: entityId};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::deleteEntity] Failed to delete entity: ${error}`);
            });
    }

    createOrUpdateEntityType(entityType, entityFields) {
        const schema = new proto.protobufs.DatabaseEntitySchema();
        schema.setName(entityType);
        schema.setFieldsList(entityFields);

        const request = new proto.protobufs.WebConfigSetEntitySchemaRequest();
        request.setSchema(schema);

        return this._server
            .send(request, proto.protobufs.WebConfigSetEntitySchemaResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigSetEntitySchemaResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::createOrUpdateEntityType] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {entityType: entityType, entityFields: entityFields};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::createOrUpdateEntityType] Failed to create or update entity type: ${error}`);
            });
    }

    createField(fieldName, fieldType) {
        const request = new proto.protobufs.WebConfigSetFieldSchemaRequest();
        request.setField( fieldName );

        const schema = new proto.protobufs.DatabaseFieldSchema();
        schema.setName( fieldName );
        schema.setType( 'protobufs.' + fieldType );
        request.setSchema( schema );

        return this._server.send(request, proto.protobufs.WebConfigSetFieldSchemaResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigSetFieldSchemaResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::createField] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {fieldName: fieldName, fieldType: fieldType};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::createField] Failed to create field: ${error}`);
            });
    }

    createSnapshot() {
        const me = this;
        const request = new proto.protobufs.WebConfigCreateSnapshotRequest();

        return me._server
            .send(request, proto.protobufs.WebConfigCreateSnapshotResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigCreateSnapshotResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::createSnapshot] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {snapshot: response.getSnapshot()};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::createSnapshot] Failed to create snapshot: ${error}`);
            });
    }

    restoreSnapshot(snapshot) {
        const me = this;
        const request = new proto.protobufs.WebConfigRestoreSnapshotRequest();
        request.setSnapshot((snapshot));

        return me._server
            .send(request, proto.protobufs.WebConfigRestoreSnapshotResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebConfigRestoreSnapshotResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::restoreSnapshot] Could not complete the request: ${response.getStatus()}`);
                }

                return {};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::restoreSnapshot] Failed to restore snapshot: ${error}`);
            });
    }

    queryRootEntityId() {
        return this._server
            .send(new proto.protobufs.WebConfigGetRootRequest(), proto.protobufs.WebConfigGetRootResponse)
            .then(response => {
                if (response.getRootid() === "") {
                    throw new Error(`[QEntityStore::queryRootEntityId] Could not complete the request: No root entity id returned`);
                }
                
                return {rootId: response.getRootid()};
            })
            .catch(error => {
                throw new Error(`[QEntityStore::queryRootEntityId] Failed to get root entity id: ${error}`);
            });
    }

    processNotifications() {
        return this._server
            .send(new proto.protobufs.WebRuntimeGetNotificationsRequest(), proto.protobufs.WebRuntimeGetNotificationsResponse)
            .then(response => {
                response.getNotificationsList().forEach(notification => {
                    this._notificationManager.dispatchEvent(notification.getToken(), notification);
                });
            })
            .catch(error => {
                qError(`[QEntityStore::processNotifications] Failed to get notifications: ${error}`);
            });
    }

    registerNotifications(nRequests, callback) {
        const request = new proto.protobufs.WebRuntimeRegisterNotificationRequest();
        request.setRequestsList(nRequests.map(r => {
            const nr = new proto.protobufs.DatabaseNotificationConfig();
            if (r.id) {
                nr.setId(r.id);
            }
            if (r.type) {
                nr.setType(r.type);
            }

            nr.setField(r.field);
            nr.setContextfieldsList(r.context || []);
            nr.setNotifyonchange(r.notifyOnChange === true);

            return nr;
        }));

        return this._server
            .send(request, proto.protobufs.WebRuntimeRegisterNotificationResponse)
            .then(response => {
                if (response.getTokensList().length !== nRequests.length) {
                    qWarn(`[QEntityStore::registerNotification] Could not complete the request: Got ${response.getTokensList().length} tokens, Expected: ${nRequests.length}`);
                }

                response.getTokensList().forEach(token => {
                    this._notificationManager.addEventListener(token, callback);
                });

                this._tokens.push(...response.getTokensList().filter(t => t !== ""));

                return {
                    tokens: response.getTokensList().filter(t => t !== ""),
                };
            })
            .catch(error => {
                throw new Error(`[QEntityStore::registerNotification] Failed to register notification: ${error}`);
            });
    }

    unregisterNotifications(tokens) {
        const request = new proto.protobufs.WebRuntimeUnregisterNotificationRequest();
        request.setTokensList(tokens);

        return this._server
            .send(request, proto.protobufs.WebRuntimeUnregisterNotificationResponse)
            .then(response => {
                if (response.getStatus() !== proto.protobufs.WebRuntimeUnregisterNotificationResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[QEntityStore::unregisterNotification] Could not complete the request: ${response.getStatus()}`);
                }

                tokens.forEach(token => {
                    this._notificationManager.removeEventListener(token);
                });

                this._tokens = this._tokens.filter(t => !tokens.includes(t));
            })
            .catch(error => {
                throw new Error(`[QEntityStore::unregisterNotification] Failed to unregister notification: ${error}`);
            });
    }

    read(dbRequest) {
        const request = new proto.protobufs.WebRuntimeDatabaseRequest();
        request.setRequesttype(proto.protobufs.WebRuntimeDatabaseRequest.RequestTypeEnum.READ);
        request.setRequestsList(dbRequest.map(r => {
            const dr = new proto.protobufs.DatabaseRequest();

            dr.setId(r.id);
            dr.setField(r.field);

            return dr;
        }));

        return this._server
            .send(request, proto.protobufs.WebRuntimeDatabaseResponse)
            .then(response => {
                return response.getResponseList();
            })
            .catch(error => {
                throw new Error(`[QEntityStore::read] Failed to read entity: ${error}`);
            });
    }

    write(dbRequest) {
        const request = new proto.protobufs.WebRuntimeDatabaseRequest();
        request.setRequesttype(proto.protobufs.WebRuntimeDatabaseRequest.RequestTypeEnum.WRITE);
        request.setRequestsList(dbRequest.map(r => {
            const dr = new proto.protobufs.DatabaseRequest();

            dr.setId(r.id);
            dr.setField(r.field);
            dr.setValue(r.value);

            return dr;
        }));

        return this._server
            .send(request, proto.protobufs.WebRuntimeDatabaseResponse)
            .then(response => {
                return response.getResponseList();
            })
            .catch(error => {
                throw new Error(`[QEntityStore::write] Failed to write entity: ${error}`);
            });
    }
}
