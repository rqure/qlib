DATABASE_EVENTS = {
    CONNECTED: "connected",
    DISCONNECTED: "disconnected",
};

class DatabaseNotificationListener {
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

class DatabaseNotificationManager {
    constructor() {
        this._listeners = {};
    }

    addEventListener(eventName, callback) {
        if (!this._listeners[eventName]) {
            this._listeners[eventName] = [];
        }

        this._listeners[eventName].push(new DatabaseNotificationListener(eventName, callback));

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

class DatabaseInteractor {
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
        this._serverInteractor = new ServerInteractor(`${location.protocol == "https:" ? "wss:" : "ws:"}//${location.hostname}${port}/ws`);
        this._notificationManager = new DatabaseNotificationManager();
        this._runInBackground = false;
        this._isConnected = null;
        this._lastConnnectionAttempt = new Date(0).getTime();
    }

    isConnected() {
        return this._isConnected;
    }

    getServerInteractor() {
        return this._serverInteractor;
    }

    getEventManager() {
        return this._notificationManager;
    }

    getAvailableFieldTypes() {
        return Object.keys(proto.qdb).filter(type => !type.startsWith("Web") && !type.startsWith("Database"));
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

        if (!this._serverInteractor.isConnected()) {
            if (this._isConnected !== false) {
                this._isConnected = false;
                this._notificationManager.dispatchEvent(DATABASE_EVENTS.DISCONNECTED, {});
            }

            const currentTime = new Date().getTime();
            if ((this._lastConnnectionAttempt + this._connectionBackoffTime) <= currentTime) {
                this._serverInteractor.connect();
                this._lastConnnectionAttempt = currentTime;
            }

            setTimeout(() => {
                this.mainLoop();
            }, this._mainLoopInterval);

            return;
        }

        this._serverInteractor
            .send(new proto.qdb.WebRuntimeGetDatabaseConnectionStatusRequest(), proto.qdb.WebRuntimeGetDatabaseConnectionStatusResponse)
            .then(response => {
                if (response.getStatus().getRaw() !== proto.qdb.ConnectionState.ConnectionStateEnum.CONNECTED) {
                    if(this._isConnected !== false) {
                        this._isConnected = false;
                        this._notificationManager.dispatchEvent(DATABASE_EVENTS.DISCONNECTED, {});
                    }
                } else {
                    if(this._isConnected !== true) {
                        this._isConnected = true;

                        /**
                         * Unregister all notifications when the connection is re-established
                         * This is to prevent duplicate notifications from being registered
                         */
                        this.unregisterNotifications(this._tokens);

                        this._notificationManager.dispatchEvent(DATABASE_EVENTS.CONNECTED, {});
                    }
                }
            })
            .catch(error => {
                qError(`[DatabaseInteractor::mainLoop] Failed to get database connection status: ${error}`);
            });
        
        this.processNotifications();

        setTimeout(() => {
            this.mainLoop();
        }, this._mainLoopInterval);
    }

    createEntity(parentId, entityName, entityType) {
        const me = this;
        const request = new proto.qdb.WebConfigCreateEntityRequest();
        request.setParentid(parentId);
        request.setName(entityName);
        request.setType(entityType);

        return me._serverInteractor
            .send(request, proto.qdb.WebConfigCreateEntityResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigCreateEntityResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::createEntity] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {entityName: entityName, entityType: entityType, parentId: parentId};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::createEntity] Failed to create entity: ${error}`);
            });
    }

    queryAllEntities(entityType) {
        const request = new proto.qdb.WebRuntimeGetEntitiesRequest();
        request.setEntitytype(entityType);

        return this._serverInteractor
            .send(request, proto.qdb.WebRuntimeGetEntitiesResponse)
            .then(response => {
                return {entities: response.getEntitiesList()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::queryAllEntities] Failed to get all entities: ${error}`);
            });
    }

    queryEntity(entityId) {
        const request = new proto.qdb.WebConfigGetEntityRequest();
        request.setId(entityId);
        
        return this._serverInteractor
            .send(request, proto.qdb.WebConfigGetEntityResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigGetEntityResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::queryEntity] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {entity: response.getEntity()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::queryEntity] Failed to get entity: ${error}`);
            });
    }

    queryEntitySchema(entityType) {
        const request = new proto.qdb.WebConfigGetEntitySchemaRequest();
        request.setType(entityType);

        return this._serverInteractor
            .send(request, proto.qdb.WebConfigGetEntitySchemaResponse)
            .then(response => {
                if(response.getStatus() !== proto.qdb.WebConfigGetEntitySchemaResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::queryEntitySchema] Could not complete the request: ${response.getStatus()}`);
                }

                return {schema: response.getSchema()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::queryEntitySchema] Failed to get entity schema: ${error}`);
            });
    }

    queryAllFields() {
        return this._serverInteractor
            .send(new proto.qdb.WebConfigGetAllFieldsRequest(), proto.qdb.WebConfigGetAllFieldsResponse)
            .then(response => {
                return {fields: response.getFieldsList()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::queryAllFields] Failed to get all fields: ${error}`);
            });
    }

    queryAllEntityTypes() {
        return this._serverInteractor
            .send(new proto.qdb.WebConfigGetEntityTypesRequest(), proto.qdb.WebConfigGetEntityTypesResponse)
            .then(response => {
                return {entityTypes: response.getTypesList()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::queryAllEntityTypes] Failed to get all entity types: ${error}`);
            });
    }

    deleteEntity(entityId) {
        const me = this;
        const request = new proto.qdb.WebConfigDeleteEntityRequest();
        request.setId(entityId);

        return me._serverInteractor
            .send(request, proto.qdb.WebConfigDeleteEntityResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigDeleteEntityResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::deleteEntity] Could not complete the request: ${response.getStatus()}`);
                }

                return {entityId: entityId};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::deleteEntity] Failed to delete entity: ${error}`);
            });
    }

    createOrUpdateEntityType(entityType, entityFields) {
        const request = new proto.qdb.WebConfigSetEntitySchemaRequest();
        request.setName(entityType);
        request.setFieldsList(entityFields);

        return this._serverInteractor
            .send(request, proto.qdb.WebConfigSetEntitySchemaResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigSetEntitySchemaResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::createOrUpdateEntityType] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {entityType: entityType, entityFields: entityFields};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::createOrUpdateEntityType] Failed to create or update entity type: ${error}`);
            });
    }

    createField(fieldName, fieldType) {
        const request = new proto.qdb.WebConfigSetFieldSchemaRequest();
        request.setField( fieldName );

        const schema = new proto.qdb.DatabaseFieldSchema();
        schema.setName( fieldName );
        schema.setType( 'qdb.' + fieldType );
        request.setSchema( schema );

        return this._serverInteractor.send(request, proto.qdb.WebConfigSetFieldSchemaResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigSetFieldSchemaResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::createField] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {fieldName: fieldName, fieldType: fieldType};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::createField] Failed to create field: ${error}`);
            });
    }

    createSnapshot() {
        const me = this;
        const request = new proto.qdb.WebConfigCreateSnapshotRequest();

        return me._serverInteractor
            .send(request, proto.qdb.WebConfigCreateSnapshotResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigCreateSnapshotResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::createSnapshot] Could not complete the request: ${response.getStatus()}`);
                }
                
                return {snapshot: response.getSnapshot()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::createSnapshot] Failed to create snapshot: ${error}`);
            });
    }

    restoreSnapshot(snapshot) {
        const me = this;
        const request = new proto.qdb.WebConfigRestoreSnapshotRequest();
        request.setSnapshot((snapshot));

        return me._serverInteractor
            .send(request, proto.qdb.WebConfigRestoreSnapshotResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebConfigRestoreSnapshotResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::restoreSnapshot] Could not complete the request: ${response.getStatus()}`);
                }

                return {};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::restoreSnapshot] Failed to restore snapshot: ${error}`);
            });
    }

    queryRootEntityId() {
        return this._serverInteractor
            .send(new proto.qdb.WebConfigGetRootRequest(), proto.qdb.WebConfigGetRootResponse)
            .then(response => {
                if (response.getRootid() === "") {
                    throw new Error(`[DatabaseInteractor::queryRootEntityId] Could not complete the request: No root entity id returned`);
                }
                
                return {rootId: response.getRootid()};
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::queryRootEntityId] Failed to get root entity id: ${error}`);
            });
    }

    processNotifications() {
        return this._serverInteractor
            .send(new proto.qdb.WebRuntimeGetNotificationsRequest(), proto.qdb.WebRuntimeGetNotificationsResponse)
            .then(response => {
                response.getNotificationsList().forEach(notification => {
                    this._notificationManager.dispatchEvent(notification.getToken(), notification);
                });
            })
            .catch(error => {
                qError(`[DatabaseInteractor::processNotifications] Failed to get notifications: ${error}`);
            });
    }

    registerNotifications(nRequests, callback) {
        const request = new proto.qdb.WebRuntimeRegisterNotificationRequest();
        request.setRequestsList(nRequests.map(r => {
            const nr = new proto.qdb.DatabaseNotificationConfig();
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

        return this._serverInteractor
            .send(request, proto.qdb.WebRuntimeRegisterNotificationResponse)
            .then(response => {
                if (response.getTokensList().length === 0) {
                    throw new Error(`[DatabaseInteractor::registerNotification] Could not complete the request: No tokens returned`);
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
                throw new Error(`[DatabaseInteractor::registerNotification] Failed to register notification: ${error}`);
            });
    }

    unregisterNotifications(tokens) {
        const request = new proto.qdb.WebRuntimeUnregisterNotificationRequest();
        request.setTokensList(tokens);

        return this._serverInteractor
            .send(request, proto.qdb.WebRuntimeUnregisterNotificationResponse)
            .then(response => {
                if (response.getStatus() !== proto.qdb.WebRuntimeUnregisterNotificationResponse.StatusEnum.SUCCESS) {
                    throw new Error(`[DatabaseInteractor::unregisterNotification] Could not complete the request: ${response.getStatus()}`);
                }

                tokens.forEach(token => {
                    this._notificationManager.removeEventListener(token);
                });

                this._tokens = this._tokens.filter(t => !tokens.includes(t));
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::unregisterNotification] Failed to unregister notification: ${error}`);
            });
    }

    read(dbRequest) {
        const request = new proto.qdb.WebRuntimeDatabaseRequest();
        request.setRequesttype(proto.qdb.WebRuntimeDatabaseRequest.RequestTypeEnum.READ);
        request.setRequestsList(dbRequest.map(r => {
            const dr = new proto.qdb.DatabaseRequest();

            dr.setId(r.id);
            dr.setField(r.field);

            return dr;
        }));

        return this._serverInteractor
            .send(request, proto.qdb.WebRuntimeDatabaseResponse)
            .then(response => {
                return response.getResponseList();
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::read] Failed to read entity: ${error}`);
            });
    }

    write(dbRequest) {
        const request = new proto.qdb.WebRuntimeDatabaseRequest();
        request.setRequesttype(proto.qdb.WebRuntimeDatabaseRequest.RequestTypeEnum.WRITE);
        request.setRequestsList(dbRequest.map(r => {
            const dr = new proto.qdb.DatabaseRequest();

            dr.setId(r.id);
            dr.setField(r.field);
            dr.setValue(r.value);

            return dr;
        }));

        return this._serverInteractor
            .send(request, proto.qdb.WebRuntimeDatabaseResponse)
            .then(response => {
                return response.getResponseList();
            })
            .catch(error => {
                throw new Error(`[DatabaseInteractor::write] Failed to write entity: ${error}`);
            });
    }
}
