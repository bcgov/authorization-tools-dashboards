set -uo pipefail
MAX_ATTEMPTS=3
RETRY_DELAY_SECONDS=0
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  echo "::group::attempt $attempt of $MAX_ATTEMPTS"
  if $CMD1 && $CMD2; then
    echo "::endgroup::"
    echo "COPY landing/index.html"
    exit 0
  fi
  echo "::endgroup::"
  if [ "$attempt" -lt "$MAX_ATTEMPTS" ]; then
    echo "::warning::Attempt $attempt failed. Retrying in ${RETRY_DELAY_SECONDS}s."
    sleep "$RETRY_DELAY_SECONDS"
  fi
done
echo "::error::failed after $MAX_ATTEMPTS attempts."
exit 1
