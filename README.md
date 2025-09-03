# Desde /srv/docker/Estado_Factura_Sunat (o donde esté el Dockerfile)
docker compose build --no-cache
docker compose up -d

# Ver logs
docker compose logs -f sunat-worker
