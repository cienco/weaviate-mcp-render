# Weaviate MCP Server (HTTP) — Render-ready

Server MCP HTTP (Streamable HTTP) per collegare **Weaviate Cloud** a client MCP remoti (es. Claude).

## Deploy rapido su Render

1. Crea un nuovo **Web Service** su Render da questo repo/cartella.
2. (Con `Dockerfile`) Render userà il container già pronto.
3. Imposta le variabili d'ambiente:
   - `WEAVIATE_URL` (oppure `WEAVIATE_CLUSTER_URL`)
   - `WEAVIATE_API_KEY`
   - (opz) `MCP_PATH` (default `/mcp/`)
4. Deploy.
5. Verifica: `GET https://<service>.onrender.com/health` → `{"status":"ok",...}`.

## Collegamento da Claude (Remote MCP)

Aggiungi un **Custom/Remote MCP server** con URL:
```
https://<service>.onrender.com/mcp/
```
e usa gli strumenti:
- `get_config`
- `check_connection`
- `list_collections`
- `get_schema(collection)`
- `keyword_search(collection, query, limit)`
- `semantic_search(collection, query, limit)`
- `hybrid_search(collection, query, alpha, limit, query_properties)`

## Note

- Per Weaviate Cloud bastano **URL + API key**.
- Il server ascolta su `0.0.0.0:$PORT` (compatibile Render).
- Health-check disponibile su `/health`.
