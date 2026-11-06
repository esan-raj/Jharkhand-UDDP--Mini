from decimal import Decimal
from pathlib import Path

from uddp.cleaning import (
    canonical_district_name,
    classify_demographic_identifiers,
    clean_datasets,
    deduplicate,
    parse_count,
    parse_number,
)

ROOT = Path(__file__).resolve().parents[1]


def demographic_row(**overrides):
    row = {
        "State name": "JHARKHAND",
        "District code": "360",
        "District name": "Hazaribagh",
        "Population": "1000",
        "Male": "400",
        "Female": "600",
        "Literate": "500",
        "Workers": "300",
        "SC": "100",
        "ST": "200",
        "Households": "250",
        "Age_Group_0_29": "500",
        "Age_Group_30_49": "300",
        "Age_Group_50": "200",
    }
    row.update(overrides)
    return row


def test_parse_number_keeps_missing_null_and_reads_indian_commas():
    assert parse_number("  ") == (None, "missing")
    assert parse_number("NA") == (None, "missing")
    assert parse_number("1,25,000") == (Decimal("125000"), None)
    assert parse_count("-12") == (-12, None)
    assert parse_number("abc")[1] == "invalid"


def test_district_aliases_are_explicit():
    assert canonical_district_name("Hazaribag") == "HAZARIBAGH"
    assert canonical_district_name("East Singhbhum") == "PURBI SINGHBHUM"
    assert canonical_district_name("West Singhbhum") == "PASHCHIMI SINGHBHUM"
    assert canonical_district_name("Seraikela") == "SARAIKELA KHARSAWAN"
    assert canonical_district_name("Saraikela-Kharsawan") == "SARAIKELA KHARSAWAN"
    assert canonical_district_name("Koderma") == "KODARMA"
    assert canonical_district_name("Not A Real District") == "NOT A REAL DISTRICT"


def test_exact_duplicates_collapse_and_conflicts_are_removed():
    kept, removed, conflicts = deduplicate(
        [{"id": 1, "v": "a"}, {"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 2, "v": "c"}],
        lambda row: row["id"],
        lambda row: row["v"],
    )
    assert removed == 1
    assert [row["id"] for row in kept] == [1]
    assert len(conflicts) == 1


def test_invalid_population_is_quarantined_and_not_zero_filled():
    result = clean_datasets(
        [
            demographic_row(Population="0"),
            demographic_row(**{"District code": "361", "District name": "Ramgarh", "Population": ""}),
        ],
        [],
        [],
        "JHARKHAND",
        "2021-22",
    )
    assert result.demographics == []
    assert {issue.rule_name for issue in result.issues} >= {
        "population_not_positive",
        "missing_required_value",
    }


def test_housing_alias_matches_census_name_and_pension_stays_state_grain():
    result = clean_datasets(
        [demographic_row()],
        [
            {
                "State Name": "JHARKHAND",
                "District Code": "",
                "District Name": "Hazaribag",
                "Tehsil Code": "00000",
                "Rural/Urban": "Total",
                "Area Name": "District - Hazaribag",
                "Total Number of households": "100",
                "Total Number of Good": "40",
                "Total Number of Livable": "55",
                "Total Number of Dilapidated": "5",
                "Latrine_premise": "25.5",
                "Alternative_Source_Open": "40",
                "O_status_O": "90",
            }
        ],
        [
            {
                "Sl. No.": "8",
                "Category": "State",
                "States/UTs": "Jharkhand",
                "Old Age Pension Scheme": "993567",
                "Widow Pension Scheme": "272108",
                "Divyangjan /Disability Pension Scheme": "31286",
            }
        ],
        "JHARKHAND",
        "2021-22",
    )
    assert result.housing[0]["district_code"] == "360"
    assert result.stats["housing_matched_by_name"] == 1
    assert len(result.pensions) == 3
    assert {row["grain"] for row in result.pensions} == {"state_scheme"}
    assert {row["beneficiary_count"] for row in result.pensions} == {993567, 272108, 31286}


def test_unmatched_district_is_not_silently_merged():
    result = clean_datasets(
        [demographic_row()],
        [
            {
                "State Name": "JHARKHAND",
                "District Code": "999",
                "District Name": "Bokaro Steel City",
                "Tehsil Code": "0",
                "Rural/Urban": "Total",
                "Area Name": "District - Bokaro Steel City",
                "Total Number of households": "100",
                "Total Number of Good": "10",
                "Total Number of Livable": "80",
                "Total Number of Dilapidated": "10",
                "Latrine_premise": "10",
                "Alternative_Source_Open": "10",
                "O_status_O": "10",
            }
        ],
        [],
        "JHARKHAND",
        "2021-22",
    )
    assert result.housing == []
    assert any(issue.rule_name == "unmatched_district" for issue in result.issues)


def test_unusable_identifiers_are_separate_from_coded_districts():
    usable, unusable = classify_demographic_identifiers(
        [
            demographic_row(),
            demographic_row(**{"District code": " ", "District name": "   "}),
        ],
        "JHARKHAND",
    )
    assert usable == ["360"]
    assert len(unusable) == 1


def test_gold_sql_does_not_spread_state_pension_across_districts():
    sql = (ROOT / "sql" / "002_gold.sql").read_text(encoding="utf-8").lower()
    assert "fact_pension_beneficiaries" not in sql
    assert "null::bigint" in sql
    assert "pension_outreach_priority_score" in sql
    assert "insert into gold.mart_district_pension_outreach" not in sql
    assert "insert into staging.mart_district_pension_outreach" in sql
