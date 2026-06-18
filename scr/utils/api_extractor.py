"""Encapsulates *how* we talk to Apify (auth, triggering an actor run,
waiting for it, paging the result dataset) so ETL scripts only describe
*what* they want. Keeps the apify-client dependency in one place.
"""
from __future__ import annotations

import logging
from typing import Any

# Token values that mean "not configured yet".
_PLACEHOLDER_MARKERS = ("REPLACE_ME", "${", "your_token", "xxx")


class ApifyExtractionError(Exception):
    '''
    Raised when an Apify actor run fails or returns no usable data.
    '''


class ApifyExtractor:
    '''
    Trigger an Apify actor and retrieve its dataset items.
    '''
    def __init__(
        self,
        api_token: str,
        actor_id: str,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._validate_token(api_token)
        self.actor_id = actor_id
        self.log = logger or logging.getLogger("madi.extract.apify")

        # Imported lazily so importing this module never hard-requires the
        # dependency (helps lightweight unit tests / linting).
        try:
            from apify_client import ApifyClient
        except ImportError as exc:  # pragma: no cover
            raise ApifyExtractionError(
                "apify-client is not installed. Add it to requirements.txt."
            ) from exc

        self.client = ApifyClient(api_token)

    @staticmethod
    def _validate_token(token: str | None) -> None:
        if not token or any(m in token for m in _PLACEHOLDER_MARKERS):
            raise ApifyExtractionError(
                "APIFY_API_TOKEN is missing or still a placeholder. "
                "Set a real token in your .env file."
            )

    def run_and_fetch(
        self,
        run_input: dict[str, Any],
        *,
        timeout_secs: int | None = None,
        memory_mbytes: int | None = None,
        build: str | None = None,
    ) -> list[dict[str, Any]]:
        '''
        Run the actor synchronously and return all dataset items.

        Raises
        ------
        ApifyExtractionError
            If the run does not succeed or yields no dataset.
        '''
        self.log.info(
            "Starting Apify actor '%s' (timeout=%ss, memory=%sMB)",
            self.actor_id,
            timeout_secs,
            memory_mbytes,
        )
        self.log.info("Actor run input: %s", run_input)

        try:
            run = self.client.actor(self.actor_id).call(
                run_input=run_input,
                timeout_secs=timeout_secs,
                memory_mbytes=memory_mbytes,
                build=build,
            )
        except Exception as exc:  # apify_client raises various errors
            raise ApifyExtractionError(
                f"Apify actor run failed to start/complete: {exc}"
            ) from exc

        if not run:
            raise ApifyExtractionError("Apify returned no run object.")

        status = run.get("status")
        run_id = run.get("id")
        dataset_id = run.get("defaultDatasetId")
        self.log.info("Actor run %s finished with status=%s", run_id, status)

        if status != "SUCCEEDED":
            raise ApifyExtractionError(
                f"Actor run {run_id} ended with status '{status}'."
            )
        if not dataset_id:
            raise ApifyExtractionError(
                f"Actor run {run_id} produced no default dataset."
            )

        items = list(self.client.dataset(dataset_id).iterate_items())
        self.log.info("Fetched %d items from dataset %s", len(items), dataset_id)
        return items
