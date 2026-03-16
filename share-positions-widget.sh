#!/bin/bash
# Script pour générer un lien invité et ouvrir le widget positions

set -e

# Configuration
BASE_URL="${1:-http://localhost:18000}"
EXPIRES_MINUTES="${2:-60}"
OPEN_BROWSER="${3:-true}"

echo "🔗 Génération d'un lien invité pour le widget positions..."
echo "Base URL: $BASE_URL"
echo "Exp. minutes: $EXPIRES_MINUTES"
echo

# Générer le token invité
RESPONSE=$(curl -sS -X POST "$BASE_URL/api/v1/strategy/trend-following/viewer/invite" \
  -H "Content-Type: application/json" \
  -d "{\"expires_minutes\":$EXPIRES_MINUTES}")

# Extraire le token
TOKEN=$(echo "$RESPONSE" | grep -o '"token":"[^"]*' | cut -d'"' -f4)

if [ -z "$TOKEN" ]; then
  echo "❌ Erreur: impossible de générer un token."
  echo "Réponse: $RESPONSE"
  exit 1
fi

# Construire l'URL du widget
WIDGET_URL="$BASE_URL/widget/positions?t=$TOKEN"

echo "✅ Token généré avec succès!"
echo
echo "📊 URL du widget positions:"
echo "$WIDGET_URL"
echo
echo "🔄 Token expire dans $EXPIRES_MINUTES minutes."
echo

# Ouvrir dans le navigateur si demandé
if [ "$OPEN_BROWSER" = "true" ]; then
  echo "🌐 Ouverture dans le navigateur..."
  if command -v open &> /dev/null; then
    # macOS
    open "$WIDGET_URL"
  elif command -v xdg-open &> /dev/null; then
    # Linux
    xdg-open "$WIDGET_URL"
  elif command -v start &> /dev/null; then
    # Windows
    start "$WIDGET_URL"
  else
    echo "⚠️  Impossible d'ouvrir le navigateur. Ouvre manuellement: $WIDGET_URL"
  fi
else
  echo "Pour ouvrir dans le navigateur, copie cette URL:"
  echo "$WIDGET_URL"
fi
