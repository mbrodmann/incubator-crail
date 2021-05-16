kubectl -n crail delete jobs `kubectl -n crail get jobs -o custom-columns=:.metadata.name`
