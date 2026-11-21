#!/usr/bin/env python3
"""Writes the provisional agent seed labels for the M0·3 eval.

This script is a record of the labelling decisions, not a substitute for the
CLI. The labels are agent-produced and provisional; the human labelling is a
separate run of `make eval-label`.
"""
import json
from pathlib import Path

AT = "2026-11-20T18:00:00Z"
LABELER = "agent:deepseek-v4.1-flash"
DATA = Path("eval/data")

articles = [json.loads(l) for l in open(DATA / "articles.candidates.jsonl")]
pairs = [json.loads(l) for l in open(DATA / "pairs.candidates.jsonl")]
by_id = {a["id"]: a for a in articles}
pair_ids = {p["id"] for p in pairs}

# (article id, category, primary candidate id|none, primary name, precision,
#  party candidate ids). The ids are stable across collector runs; the order
#  in the candidates file is not.
ARTICLE_LABELS = [
    ('a9b9ee78ddf96', 'conflict', 'country:BD', 'Bangladesh', 'country', ['country:MM']),
    ('a6097d57c52da', 'tech', 'none', '', 'none', ['country:RU', 'country:CN', 'country:IR', 'country:KP']),
    ('a93613ac953c8', 'health', 'country:BR', 'Brazil', 'country', []),
    ('a1ef61dc716cf', 'politics', '2950159', 'Berlin', 'city', ['country:DE']),
    ('a3d4928456c2e', 'conflict', '703448', 'Kyiv', 'city', ['country:UA', 'country:RU']),
    ('addbe388f9d28', 'politics', '1880252', 'Singapore', 'city', ['1880252', 'country:CN']),
    ('a87a03058b822', 'conflict', '108410', 'Riyadh', 'city', ['108410', 'country:SA']),
    ('ac309d8d8329c', 'sports', '2158177', 'Melbourne', 'city', []),
    ('a66dfca0112fe', 'economy', '1819729', 'Hong Kong', 'city', ['1819729', 'country:CN']),
    ('ab4a02b67a5b0', 'politics', 'country:CA', 'Canada', 'country', ['country:CA']),
    ('ac8aa98c984c0', 'conflict', 'none', '', 'none', ['country:IL']),
    ('a446bdbbb1693', 'conflict', 'country:MM', 'Myanmar', 'country', ['country:MM']),
    ('a663befa0c387', 'politics', 'none', '', 'none', []),
    ('a4634d2c6ccf0', 'sports', 'none', '', 'none', ['country:CM']),
    ('af134fe4f942f', 'climate', 'country:ZA', 'South Africa', 'country', ['country:ZA', 'country:GE']),
    ('a03427f29ff60', 'politics', '2988507', 'Paris', 'city', ['country:FR']),
    ('a343a36ead413', 'tech', 'none', '', 'none', ['country:RU', 'country:IL']),
    ('a39a89cfa3e3c', 'politics', 'none', '', 'none', []),
    ('a4379e92f4c62', 'other', '2165798', 'Geelong', 'city', []),
    ('a74cda6868d18', 'disaster', '5368361', 'Los Angeles', 'city', ['5368361']),
    ('a90ecb5dd01ae', 'politics', 'none', '', 'none', []),
    ('a6a53385fc1ba', 'conflict', '703448', 'Kyiv', 'city', ['703448', 'country:UA']),
    ('ad62915d5b658', 'politics', 'country:IR', 'Iran', 'country', ['country:IR']),
    ('af00d247b061f', 'sports', 'none', '', 'none', ['country:MA', 'country:FJ']),
    ('a03898296a05c', 'politics', '2422465', 'Conakry', 'city', ['country:GN', 'country:BF']),
    ('a7276f5184fa3', 'disaster', '1715348', 'Davao', 'city', []),
    ('a964eef1c83b1', 'economy', 'none', '', 'none', []),
    ('a61d81cd00f35', 'politics', 'none', '', 'none', ['country:AZ', 'country:DE']),
    ('a57839f0caff6', 'economy', '4140963', 'Washington', 'city', []),
    ('ab4af86b6cf84', 'disaster', '5368361', 'Los Angeles', 'city', ['5368361']),
]

# (pair id, related, type, direction, strength, duplicate)
PAIR_LABELS = [
    # same-event coverage: clustering handles it in production, so eval skips it
    ("p-a4bdf044a40fa-a789d09fa957d", True, "thematic", "unclear", 3, True),
    ("p-a03427f29ff60-a7c64ff706afc", True, "thematic", "unclear", 3, True),
    ("p-a14bc82f747ce-ab8fc44a4e759", True, "thematic", "unclear", 3, True),
    ("p-a7c408c6ff522-a25ab10b19975", True, "thematic", "unclear", 3, True),
    ("p-aaeff9386d011-a71591d4c3a46", True, "thematic", "unclear", 3, True),
    ("p-a8e3ceec613f9-a3d617bdf334a", True, "thematic", "unclear", 3, True),
    ("p-ad10cc5570c24-a036ce383e805", True, "thematic", "unclear", 3, True),
    ("p-afbff5ae02f69-acb8c3ef54bbf", True, "thematic", "unclear", 3, True),
    ("p-a9a22b78ceb6b-a96005882dc90", True, "thematic", "unclear", 3, True),
    ("p-a9b9ee78ddf96-aaeff9386d011", True, "thematic", "unclear", 3, True),
    # distinct events with a direct or indirect connection
    ("p-a82a1a046161f-a4dffa5bc2cfc", True, "reaction", "b_to_a", 2, False),
    ("p-a3d4928456c2e-a3fcc04ba00f0", True, "thematic", "unclear", 1, False),
    ("p-a3d4928456c2e-a5cb09b8a8cc2", True, "thematic", "unclear", 1, False),
    ("p-a14bc82f747ce-a633d3b37de6e", True, "reaction", "a_to_b", 2, False),
    ("p-a14bc82f747ce-a82a1a046161f", True, "reaction", "a_to_b", 2, False),
    ("p-a14bc82f747ce-a583eebda9241", True, "reaction", "a_to_b", 2, False),
    ("p-a9b9ee78ddf96-aa9cefeb13011", True, "thematic", "unclear", 1, False),
    ("p-a446bdbbb1693-aa9cefeb13011", True, "thematic", "unclear", 1, False),
    ("p-a446bdbbb1693-a83035937eb8b", True, "thematic", "unclear", 1, False),
    ("p-aa9cefeb13011-a83035937eb8b", True, "thematic", "unclear", 1, False),
    # unrelated
    ("p-a30481de05e8a-a0f144ace9611", False, "none", "unclear", 0, False),
    ("p-ac8aa98c984c0-a684f2c8b49d1", False, "none", "unclear", 0, False),
    ("p-a22b6435120a8-ab544152bffcf", False, "none", "unclear", 0, False),
    ("p-a43dd820136b6-a431bce6b89d2", False, "none", "unclear", 0, False),
    ("p-a283c4f7c9992-ab2e2ea59ba24", False, "none", "unclear", 0, False),
    ("p-a446bdbbb1693-a9f046dec6d52", False, "none", "unclear", 0, False),
    ("p-a26181567a70c-ab2e2ea59ba24", False, "none", "unclear", 0, False),
    ("p-a23d25a05deab-a7c79267f44aa", False, "none", "unclear", 0, False),
    ("p-ade0c6d311e75-a85a95e603455", False, "none", "unclear", 0, False),
    ("p-aa9cefeb13011-a8808991f5b81", False, "none", "unclear", 0, False),
]


def main():
    out_articles = []
    for aid, cat, pid, pname, prec, parties in ARTICLE_LABELS:
        a = by_id[aid]
        cand_ids = {c["id"] for c in a.get("candidates", [])}
        if pid != "none" and pid not in cand_ids:
            raise SystemExit(f"article {a['id']}: primary {pid} not in candidates")
        for p in parties:
            if p not in cand_ids:
                raise SystemExit(f"article {a['id']}: party {p} not in candidates")
        out_articles.append({
            "article_id": a["id"],
            "category": cat,
            "primary_place": pid,
            "primary_name": pname,
            "precision": prec,
            "parties": parties,
            "labeler": LABELER,
            "labeled_at": AT,
            "provisional": True,
        })
    assert len(out_articles) == 30
    out_pairs = []
    for pid, related, typ, direction, strength, duplicate in PAIR_LABELS:
        if pid not in pair_ids:
            raise SystemExit(f"unknown pair {pid}")
        row = {
            "pair_id": pid,
            "related": related,
            "type": typ,
            "direction": direction,
            "strength": strength,
            "labeler": LABELER,
            "labeled_at": AT,
            "provisional": True,
        }
        if duplicate:
            row["duplicate"] = True
        out_pairs.append(row)
    assert len(out_pairs) == 30
    with open(DATA / "articles.labels.jsonl", "w") as f:
        for r in out_articles:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(DATA / "pairs.labels.jsonl", "w") as f:
        for r in out_pairs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("wrote 30 article labels and 30 pair labels (10 duplicate pairs)")


if __name__ == "__main__":
    main()
