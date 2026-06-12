# Makefile para ejecutar la aplicación Reddit NER con diferentes configuraciones.

# Phony targets para indicar que no son archivos.
.PHONY: all run run-local run-malformed run-bad-url run-incorrect-format test help

# El target por defecto que se ejecuta con 'make'
all: run

# Target principal para correr la aplicación con la configuración por defecto.
run: run-local

# --- Targets para diferentes archivos de suscripciones ---

# Ejecuta con el archivo de suscripciones local y válido.
run-local:
	@echo "▶️  Ejecutando con suscripciones locales (data/local_subscriptions.json)..."
	@sbt "run --subscription-file data/local_subscriptions.json"

# Ejecuta con un archivo JSON malformado para probar el manejo de errores.
run-malformed:
	@echo "▶️  Ejecutando con JSON malformado (data/malformed_json_subscriptions.json)..."
	@sbt "run --subscription-file data/malformed_json_subscriptions.json"

# Ejecuta con un archivo que contiene URLs inválidas o inaccesibles.
run-bad-url:
	@echo "▶️  Ejecutando con URLs inválidas (data/bad_url_subscriptions.json)..."
	@sbt "run --subscription-file data/bad_url_subscriptions.json"

# Ejecuta con un archivo donde a las suscripciones les faltan campos.
run-incorrect-format:
	@echo "▶️  Ejecutando con formato de suscripción incorrecto (data/incorrect_format_subscriptions.json)..."
	@sbt "run --subscription-file data/incorrect_format_subscriptions.json"

# Ejecuta los tests de integración.
test:
	@echo "▶️  Ejecutando los tests de integración..."
	@bash tests.sh

# Target de ayuda para mostrar los comandos disponibles.
help:
	@echo "Comandos disponibles:"
	@echo "  make run                 - Ejecuta la configuración por defecto (local)."
	@echo "  make run-local           - Ejecuta con el archivo de suscripciones local."
	@echo "  make run-malformed       - Ejecuta con un archivo JSON inválido."
	@echo "  make run-bad-url         - Ejecuta con URLs fallidas."
	@echo "  make run-incorrect-format- Ejecuta con suscripciones con campos faltantes."
	@echo "  make test                - Ejecuta los tests de integración (tests.sh)."
