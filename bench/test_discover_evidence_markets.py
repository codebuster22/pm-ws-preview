import io
import contextlib
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import discover_evidence_markets as discovery
import discover_polymarket as polymarket


def leaf(slug, condition, end=10_000.9, **extra):
    return {"slug": slug, "conditionId": condition, "tradeType": "clob",
            "marketType": "single", "expired": False, "startAt": 1,
            "expirationTimestamp": end, **extra}


def poly(tags):
    return {"active": True, "closed": False, "enableOrderBook": True,
            "acceptingOrders": True, "conditionId": "condition", "clobTokenIds": '["yes","no"]',
            "outcomes": '["Yes","No"]', "sportsMarketType": None, "tags": tags}


class LimitlessCandidatesTest(unittest.TestCase):
    def test_polymarket_tag_exclusion_uses_tags_not_sports_market_type(self):
        esports = poly([{"slug": "esports"}])
        politics = poly([{"slug": "politics"}])
        excluded = {"sports", "esports"}
        esports_tags = polymarket.tag_slugs(esports, 0)
        self.assertEqual(excluded.intersection(esports_tags), {"esports"})
        self.assertFalse(excluded.intersection(polymarket.tag_slugs(politics, 1)))
        with self.assertRaises(polymarket.DiscoveryError):
            polymarket.tag_slugs(poly(["esports"]), 2)

    def test_reused_polymarket_requires_tag_evidence_for_tag_exclusions(self):
        selection = {"polymarket": [{"condition_id": "condition", "clob_token_ids": ["yes", "no"],
                                      "end_epoch": 10_000}]}
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "selection.json"
            path.write_text(json.dumps(selection), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "lacks tag evidence"):
                discovery.reused_polymarket(path, 2, 1, {"sports"})

    def test_polymarket_keyset_tags_are_opt_in(self):
        urls = []

        def response(request, timeout):
            urls.append(request.full_url)
            return io.BytesIO(b'{"markets":[]}')

        with mock.patch.object(polymarket.urllib.request, "urlopen", side_effect=response):
            polymarket.fetch_page(None, 100, 30)
            polymarket.fetch_page(None, 100, 30, include_tags=True)
        self.assertNotIn("include_tag=true", urls[0])
        self.assertIn("include_tag=true", urls[1])

    def test_cli_excludes_esports_by_tag_before_selecting_top_hundred(self):
        def market(index, tags, volume):
            return {"active": True, "closed": False, "enableOrderBook": True,
                    "acceptingOrders": True, "conditionId": f"p{index}",
                    "clobTokenIds": json.dumps([f"yes{index}", f"no{index}"]),
                    "outcomes": '["Yes","No"]', "slug": f"p{index}",
                    "endDate": "1970-01-01T03:00:00Z", "volume24hr": volume,
                    "sportsMarketType": None, "tags": tags}

        limitless = {"limitless": [
            {"slug": f"l{index}", "condition_id": f"lcondition{index}", "market_kind": "leaf",
             "end_epoch": 10_000, "start_epoch": 1, "volume": 0}
            for index in range(100)]}
        rows = [market(index, [{"slug": "politics"}], index) for index in range(100)]
        rows.append(market(100, [{"slug": "esports"}], 1_000_000))
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            limitless_path, output = root / "limitless.json", root / "selection.json"
            limitless_path.write_text(json.dumps(limitless), encoding="utf-8")
            argv = ["discover_evidence_markets.py", "--output", str(output), "--limitless-pages", "0",
                    "--reuse-limitless-selection", str(limitless_path), "--polymarket-pages", "1",
                    "--polymarket-exclude-tag", "sports", "--polymarket-exclude-tag", "esports",
                    "--min-remaining-seconds", "1"]
            with mock.patch.object(sys, "argv", argv), \
                    mock.patch.object(discovery.time, "time", return_value=10), \
                    mock.patch.object(discovery.polymarket, "fetch_page", return_value=(rows, None)) as fetch:
                with contextlib.redirect_stdout(io.StringIO()):
                    discovery.main()
            result = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(fetch.call_args.args, (None, 100, 30, True))
        self.assertEqual(len(result["polymarket"]), 100)
        self.assertNotIn("p100", {row["condition_id"] for row in result["polymarket"]})
        self.assertTrue(all(row["tag_slugs"] == ["politics"] for row in result["polymarket"]))
        self.assertEqual(result["polymarket_excluded_tag_counts"], {"sports": 0, "esports": 1})

    def test_flattens_only_eligible_clob_leaves(self):
        valid_a, valid_b = leaf("daily-a", "a"), leaf("daily-b", "b", marketType="group")
        rows = [{"slug": "group", "conditionId": "parent", "tradeType": "clob",
                 "marketType": "group", "markets": [valid_a, valid_b,
                     leaf("empty", "",), leaf("no-expiry", "c", expirationTimestamp=None),
                     leaf("resolved", "d", expired=True)]},
                leaf("standalone", "e")]
        selected, excluded = discovery.limitless_candidates(rows, 2, 1, False)
        self.assertEqual(set(selected), {"daily-a", "daily-b", "standalone"})
        self.assertEqual({row["condition_id"] for row in selected.values()}, {"a", "b", "e"})
        self.assertEqual(selected["daily-a"]["end_epoch"], 10_000)
        self.assertEqual(selected["daily-b"]["market_type"], "group")
        self.assertEqual(selected["daily-b"]["market_kind"], "leaf")
        self.assertFalse(excluded)

    def test_container_and_match_exclusion_never_select_parent(self):
        rows = [{"slug": "parent", "tradeType": "clob", "markets": []},
                {"title": "Team A vs Team B", "markets": [leaf("team-a-vs-team-b", "a")]},
                leaf("up-or-down-daily", "b")]
        selected, excluded = discovery.limitless_candidates(rows, 2, 1, True)
        self.assertEqual(set(selected), {"up-or-down-daily"})
        self.assertEqual(excluded, {"team-a-vs-team-b"})

    def test_match_exclusion_covers_both_to_score_questions(self):
        rows = [leaf("both-score", "a", title="Team A and Team B both to score?"),
                leaf("both-teams", "b", title="Will both teams to score?"),
                leaf("daily", "c", title="BTC Up or Down - Daily")]
        selected, excluded = discovery.limitless_candidates(rows, 2, 1, True)
        self.assertEqual(set(selected), {"daily"})
        self.assertEqual(excluded, {"both-score", "both-teams"})

    def test_match_exclusion_covers_total_goals_corners_and_cards(self):
        rows = [leaf("goals", "a", title="Total Goals"),
                leaf("corners", "b", title="Total Corners"),
                leaf("cards", "c", title="Total Cards"),
                leaf("daily", "d", title="BTC Up or Down - Daily")]
        selected, excluded = discovery.limitless_candidates(rows, 2, 1, True)
        self.assertEqual(set(selected), {"daily"})
        self.assertEqual(excluded, {"goals", "corners", "cards"})

    def test_reused_selection_merges_validated_leaves_and_records_provenance(self):
        selection = {"discovered_at": "2026-09-12T16:00:00+00:00", "limitless": [
            {"slug": "daily", "title": "BTC Up or Down - Daily", "condition_id": "a",
             "market_kind": "leaf", "end_epoch": 10_000, "start_epoch": 1, "volume": 2},
            {"slug": "corners", "title": "Total Corners", "condition_id": "b",
             "market_kind": "leaf", "end_epoch": 10_000, "start_epoch": 1, "volume": 1}]}
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "selection.json"
            path.write_text(json.dumps(selection), encoding="utf-8")
            selected, excluded, provenance = discovery.reused_limitless(path, 2, 1, True)
            self.assertEqual(set(selected), {"daily"})
            self.assertEqual(excluded, {"corners"})
            self.assertEqual(provenance["old_discovered_at"], selection["discovered_at"])
            self.assertEqual(len(provenance["sha256"]), 64)
            self.assertEqual(provenance["venue_status"], "not_refreshed")

    def test_raw_descriptor_cache_format_is_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "cache.json"
            path.write_text(json.dumps({"rows": [leaf("daily", "a")]}), encoding="utf-8")
            rows, provenance = discovery.cached_rows(path)
            self.assertEqual(rows, [leaf("daily", "a")])
            self.assertEqual(provenance["rows"], 1)


if __name__ == "__main__":
    unittest.main()
