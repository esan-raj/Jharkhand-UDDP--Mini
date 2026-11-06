import re
from dataclasses import dataclass, field
from decimal import Decimal

MISSING_TOKENS = {"", "na", "n/a", "null", "none", "-", "nan", "n.a."}

# Verified aliases. Bare Saraikela/Seraikela is the Census district Saraikela-Kharsawan,
# not a fuzzy match against a different district.
ALIASES = {
    "HAZARIBAG": "HAZARIBAGH",
    "EAST SINGHBHUM": "PURBI SINGHBHUM",
    "WEST SINGHBHUM": "PASHCHIMI SINGHBHUM",
    "PASCHIMI SINGHBHUM": "PASHCHIMI SINGHBHUM",
    "SARAIKELA": "SARAIKELA KHARSAWAN",
    "SERAIKELA": "SARAIKELA KHARSAWAN",
    "SERAIKELA KHARSAWAN": "SARAIKELA KHARSAWAN",
    "SARAIKELA KHARSAWAN": "SARAIKELA KHARSAWAN",
    "KODERMA": "KODARMA",
    "SAHEBGANJ": "SAHIBGANJ",
}

ALIAS_NOTES = {
    "HAZARIBAG": "Spelling variant of Hazaribagh",
    "EAST SINGHBHUM": "English name for Purbi Singhbhum",
    "WEST SINGHBHUM": "English name for Pashchimi Singhbhum",
    "PASCHIMI SINGHBHUM": "Spelling variant of Pashchimi Singhbhum",
    "SARAIKELA": "Short name of Saraikela-Kharsawan",
    "SERAIKELA": "Spelling variant of Saraikela-Kharsawan",
    "SERAIKELA KHARSAWAN": "Spelling variant of Saraikela-Kharsawan",
    "SARAIKELA KHARSAWAN": "Census spelling Saraikela-Kharsawan",
    "KODERMA": "Spelling variant of Kodarma",
    "SAHEBGANJ": "Spelling variant of Sahibganj",
}

PENSION_SCHEMES = (
    (
        "Old Age Pension Scheme",
        "old_age_pension_scheme",
        "Indira Gandhi National Old Age Pension Scheme",
    ),
    (
        "Widow Pension Scheme",
        "widow_pension_scheme",
        "Indira Gandhi National Widow Pension Scheme",
    ),
    (
        "Divyangjan /Disability Pension Scheme",
        "divyangjan__disability_pension_scheme",
        "Indira Gandhi National Disability Pension Scheme",
    ),
)

DEMOGRAPHIC_FIELDS = (
    "State name",
    "District name",
    "District code",
    "Population",
    "Male",
    "Female",
    "Literate",
    "Workers",
    "SC",
    "ST",
    "Households",
    "Age_Group_0_29",
    "Age_Group_30_49",
    "Age_Group_50",
)

HOUSING_FIELDS = (
    "State Name",
    "District Name",
    "District Code",
    "Total Number of households",
    "Total Number of Good",
    "Total Number of Livable",
    "Total Number of Dilapidated",
    "Latrine_premise",
    "Alternative_Source_Open",
    "O_status_O",
)


@dataclass
class Issue:
    source_name: str
    rule_name: str
    severity: str
    business_key: str
    details: dict


@dataclass
class CleanOutput:
    districts: list = field(default_factory=list)
    demographics: list = field(default_factory=list)
    housing: list = field(default_factory=list)
    pensions: list = field(default_factory=list)
    issues: list = field(default_factory=list)
    stats: dict = field(default_factory=dict)


def is_blank(value):
    if value is None:
        return True
    return str(value).strip().lower() in MISSING_TOKENS


def field(row, *names):
    lookup = {str(key).strip().lower(): key for key in row}
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
        key = lookup.get(name.strip().lower())
        if key is not None and row[key] is not None:
            return row[key]
    return None


def parse_number(value):
    """Parse a source number without replacing blanks with zero."""
    if is_blank(value):
        return None, "missing"
    text = str(value).strip()
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()
    if text.startswith("+"):
        text = text[1:].strip()
    elif text.startswith("-"):
        negative = True
        text = text[1:].strip()
    text = text.replace(",", "").replace(" ", "")
    if not re.fullmatch(r"\d+(\.\d+)?", text):
        return None, "invalid"
    number = Decimal(text)
    if negative:
        number = -number
    return number, None


def parse_count(value):
    number, status = parse_number(value)
    if status:
        return None, status
    if number != number.to_integral_value():
        return None, "invalid"
    return int(number), None


def parse_percent(value):
    number, status = parse_number(value)
    if status:
        return None, status
    if number < 0 or number > 100:
        return None, "out_of_range"
    return number, None


def normalize_name(value):
    if is_blank(value):
        return ""
    text = str(value).upper().replace("&", " AND ")
    text = text.replace("-", " ")
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if text.startswith("DISTRICT "):
        text = text[len("DISTRICT ") :].strip()
    return text


def canonical_district_name(value):
    normalized = normalize_name(value)
    return ALIASES.get(normalized, normalized)


def canonical_code(value):
    if is_blank(value):
        return ""
    text = str(value).strip()
    if re.fullmatch(r"\d+", text):
        return str(int(text))
    return text


def is_state(value, state_name):
    return normalize_name(value) == normalize_name(state_name)


def is_district_housing_row(row):
    tehsil = canonical_code(field(row, "Tehsil Code"))
    area_type = normalize_name(field(row, "Rural/Urban"))
    area_name = field(row, "Area Name")
    looks_like_district = not is_blank(area_name) and str(area_name).strip().lower().startswith("district")
    return tehsil in {"", "0"} and area_type == "TOTAL" and looks_like_district


def classify_demographic_identifiers(rows, state_name):
    """Split Jharkhand demographic rows before quarantine.

    Usable codes are the publication contract. Rows with no district code or no
    district name are reported separately and are not treated as a district code.
    """
    usable = []
    unusable = []
    for row in rows:
        if not is_state(field(row, "State name", "State Name"), state_name):
            continue
        code = canonical_code(field(row, "District code"))
        name = field(row, "District name")
        if not code or is_blank(name):
            unusable.append(
                {
                    "district_code": code or None,
                    "district_name": None if is_blank(name) else str(name),
                }
            )
            continue
        usable.append(code)
    return sorted(set(usable)), unusable


def deduplicate(rows, key_fn, payload_fn):
    """Keep one exact copy. Quarantine every row when the same key has different values."""
    groups = {}
    for row in rows:
        groups.setdefault(key_fn(row), []).append(row)
    kept = []
    exact_removed = 0
    conflicts = []
    for key, items in groups.items():
        if len(items) == 1:
            kept.append(items[0])
            continue
        payloads = {payload_fn(item) for item in items}
        if len(payloads) == 1:
            kept.append(items[0])
            exact_removed += len(items) - 1
        else:
            conflicts.append((key, items))
    return kept, exact_removed, conflicts


def missing_profile(rows, columns):
    profile = {}
    for column in columns:
        profile[column] = sum(1 for row in rows if is_blank(field(row, column)))
    return profile


def _issue(issues, source_name, rule_name, severity, business_key, **details):
    issues.append(
        Issue(
            source_name=source_name,
            rule_name=rule_name,
            severity=severity,
            business_key=str(business_key),
            details=details,
        )
    )


def _demo_key(row):
    code = canonical_code(field(row, "District code"))
    if code:
        return f"code:{code}"
    return f"name:{canonical_district_name(field(row, 'District name'))}"


def _demo_payload(row):
    return tuple(field(row, column) for column in DEMOGRAPHIC_FIELDS)


def _housing_key(row):
    code = canonical_code(field(row, "District Code", "District code"))
    if code:
        return f"code:{code}"
    return f"name:{canonical_district_name(field(row, 'District Name', 'District name'))}"


def _housing_payload(row):
    return tuple(field(row, column) for column in HOUSING_FIELDS)


def _pension_key(row):
    return normalize_name(field(row, "States/UTs", "states_uts"))


def _pension_payload(row):
    return tuple(
        field(row, label, api_id) for label, api_id, _scheme in PENSION_SCHEMES
    ) + (field(row, "Category", "category"),)


def _parse_demo_measures(row, issues, business_key):
    spec = (
        ("Population", "total_population", True),
        ("Male", "male_population", False),
        ("Female", "female_population", False),
        ("Literate", "literate_population", False),
        ("Workers", "total_workers", False),
        ("SC", "sc_population", False),
        ("ST", "st_population", False),
        ("Households", "total_households", False),
        ("Age_Group_0_29", "age_group_0_29", False),
        ("Age_Group_30_49", "age_group_30_49", False),
        ("Age_Group_50", "age_group_50_plus", False),
    )
    parsed = {}
    for column, target, required in spec:
        number, status = parse_count(field(row, column))
        if status == "invalid":
            _issue(
                issues,
                "demographic",
                "invalid_numeric",
                "error",
                business_key,
                column=column,
                value=field(row, column),
            )
            return None
        if status == "missing" and required:
            _issue(
                issues,
                "demographic",
                "missing_required_value",
                "error",
                business_key,
                column=column,
            )
            return None
        if number is not None and number < 0:
            _issue(
                issues,
                "demographic",
                "negative_value",
                "error",
                business_key,
                column=column,
                value=number,
            )
            return None
        parsed[target] = number
    return parsed


def _check_demographic_consistency(parsed, issues, business_key):
    total = parsed["total_population"]
    male = parsed["male_population"]
    female = parsed["female_population"]
    if male is not None and female is not None and male + female != total:
        _issue(
            issues,
            "demographic",
            "sex_total_mismatch",
            "warning",
            business_key,
            male=male,
            female=female,
            total=total,
            difference=(male + female) - total,
        )
    households = parsed["total_households"]
    if households is not None and households > total:
        _issue(
            issues,
            "demographic",
            "households_exceed_population",
            "warning",
            business_key,
            households=households,
            population=total,
        )
    for key, rule in (
        ("sc_population", "sc_exceeds_population"),
        ("st_population", "st_exceeds_population"),
    ):
        value = parsed[key]
        if value is not None and value > total:
            _issue(
                issues,
                "demographic",
                rule,
                "warning",
                business_key,
                value=value,
                population=total,
            )
    sc = parsed["sc_population"]
    st = parsed["st_population"]
    if sc is not None and st is not None and sc + st > total:
        _issue(
            issues,
            "demographic",
            "sc_plus_st_exceeds_population",
            "warning",
            business_key,
            sc=sc,
            st=st,
            population=total,
        )


def _parse_percent_field(row, column, issues, business_key):
    number, status = parse_percent(field(row, column))
    if status == "missing":
        return None
    if status in {"invalid", "out_of_range"}:
        _issue(
            issues,
            "housing",
            "invalid_percent",
            "error",
            business_key,
            column=column,
            value=field(row, column),
            status=status,
        )
        return "invalid"
    return number


def clean_datasets(demographic_rows, housing_rows, pension_rows, state_name, reference_period):
    issues = []
    demographic_state = [
        row
        for row in demographic_rows
        if is_state(field(row, "State name", "State Name"), state_name)
    ]
    housing_state = [
        row
        for row in housing_rows
        if is_state(field(row, "State Name", "State name"), state_name)
    ]
    housing_district = []
    for row in housing_state:
        if is_district_housing_row(row):
            housing_district.append(row)
        else:
            _issue(
                issues,
                "housing",
                "wrong_grain",
                "error",
                field(row, "District Code", "Area Name"),
                area_name=field(row, "Area Name"),
                rural_urban=field(row, "Rural/Urban"),
            )
    pension_state = [
        row
        for row in pension_rows
        if is_state(field(row, "States/UTs", "states_uts"), state_name)
    ]

    demo_kept, demo_exact, demo_conflicts = deduplicate(
        demographic_state, _demo_key, _demo_payload
    )
    housing_kept, housing_exact, housing_conflicts = deduplicate(
        housing_district, _housing_key, _housing_payload
    )
    pension_kept, pension_exact, pension_conflicts = deduplicate(
        pension_state, _pension_key, _pension_payload
    )
    for source, conflicts in (
        ("demographic", demo_conflicts),
        ("housing", housing_conflicts),
        ("pension", pension_conflicts),
    ):
        for key, items in conflicts:
            _issue(
                issues,
                source,
                "conflicting_duplicate",
                "error",
                key,
                copies=len(items),
            )

    districts = []
    demographics = []
    names_by_code = {}
    codes_by_name = {}
    for row in demo_kept:
        raw_name = field(row, "District name")
        code = canonical_code(field(row, "District code"))
        business_key = code or normalize_name(raw_name)
        if is_blank(raw_name):
            _issue(issues, "demographic", "null_district_name", "error", business_key)
            continue
        if not code:
            _issue(
                issues,
                "demographic",
                "missing_district_code",
                "error",
                canonical_district_name(raw_name),
            )
            continue
        parsed = _parse_demo_measures(row, issues, code)
        if parsed is None:
            continue
        if parsed["total_population"] is None or parsed["total_population"] <= 0:
            _issue(
                issues,
                "demographic",
                "population_not_positive",
                "error",
                code,
                value=parsed["total_population"],
            )
            continue
        _check_demographic_consistency(parsed, issues, code)
        standard_name = canonical_district_name(raw_name)
        names_by_code[code] = standard_name
        codes_by_name[standard_name] = code
        districts.append(
            {
                "district_code": code,
                "district_name": str(raw_name).strip(),
                "state_name": state_name.title() if state_name.isupper() else state_name,
                "source_name_raw": str(raw_name).strip(),
                "name_match_method": "source_district_code",
            }
        )
        demographics.append({"district_code": code, **parsed})

    housing = []
    matched_by_code = 0
    matched_by_name = 0
    for row in housing_kept:
        raw_name = field(row, "District Name", "District name")
        code = canonical_code(field(row, "District Code", "District code"))
        standard_name = canonical_district_name(raw_name)
        business_key = code or standard_name
        if code and code in names_by_code:
            if standard_name and standard_name != names_by_code[code]:
                _issue(
                    issues,
                    "housing",
                    "name_code_disagreement",
                    "warning",
                    code,
                    housing_name=raw_name,
                    demographic_name=names_by_code[code],
                )
            matched_by_code += 1
        elif standard_name in codes_by_name:
            code = codes_by_name[standard_name]
            matched_by_name += 1
        else:
            _issue(
                issues,
                "housing",
                "unmatched_district",
                "error",
                business_key,
                district_name=raw_name,
                district_code=code,
            )
            continue

        measures = {}
        invalid = False
        for column, target in (
            ("Total Number of Good", "pct_good"),
            ("Total Number of Livable", "pct_livable"),
            ("Total Number of Dilapidated", "pct_dilapidated"),
            ("Latrine_premise", "pct_latrine_within_premises"),
            ("Alternative_Source_Open", "pct_open_defecation"),
            ("O_status_O", "pct_owned"),
        ):
            parsed = _parse_percent_field(row, column, issues, code)
            if parsed == "invalid":
                invalid = True
                break
            measures[target] = parsed
        if invalid:
            continue
        base, base_status = parse_number(field(row, "Total Number of households"))
        if base_status == "invalid":
            _issue(
                issues,
                "housing",
                "invalid_numeric",
                "error",
                code,
                column="Total Number of households",
                value=field(row, "Total Number of households"),
            )
            continue
        if base is not None and base != Decimal("100"):
            _issue(
                issues,
                "housing",
                "unexpected_household_base",
                "warning",
                code,
                value=str(base),
            )
        housing.append(
            {
                "district_code": code,
                "district_name_raw": None if is_blank(raw_name) else str(raw_name).strip(),
                "indicator_unit": "percent_of_households",
                "published_household_base": None if base is None else base,
                **measures,
            }
        )

    pensions = []
    for row in pension_kept:
        state = str(field(row, "States/UTs", "states_uts")).strip()
        category = field(row, "Category", "category")
        serial = field(row, "Sl. No.", "sl__no_")
        for label, api_id, scheme_name in PENSION_SCHEMES:
            raw_value = field(row, label, api_id)
            number, status = parse_count(raw_value)
            business_key = f"{state}|{scheme_name}"
            if status == "invalid":
                _issue(
                    issues,
                    "pension",
                    "invalid_numeric",
                    "error",
                    business_key,
                    value=raw_value,
                )
                continue
            if number is not None and number < 0:
                _issue(
                    issues,
                    "pension",
                    "negative_beneficiary_count",
                    "error",
                    business_key,
                    value=number,
                )
                continue
            pensions.append(
                {
                    "state_name": state,
                    "scheme_name": scheme_name,
                    "beneficiary_count": number,
                    "category": None if is_blank(category) else str(category).strip(),
                    "reference_period": reference_period,
                    "grain": "state_scheme",
                    "source_sl_no": None if is_blank(serial) else str(serial).strip(),
                }
            )

    stats = {
        "demographic_rows_in_file": len(demographic_rows),
        "demographic_rows_filtered": len(demographic_state),
        "demographic_exact_duplicates_removed": demo_exact,
        "demographic_conflicting_keys": len(demo_conflicts),
        "demographic_rows_after_dedup": len(demo_kept),
        "demographic_rows_published": len(demographics),
        "demographic_missing_values": missing_profile(demographic_state, DEMOGRAPHIC_FIELDS),
        "housing_rows_in_file": len(housing_rows),
        "housing_rows_filtered_state": len(housing_state),
        "housing_rows_district_grain": len(housing_district),
        "housing_exact_duplicates_removed": housing_exact,
        "housing_conflicting_keys": len(housing_conflicts),
        "housing_rows_after_dedup": len(housing_kept),
        "housing_rows_published": len(housing),
        "housing_matched_by_code": matched_by_code,
        "housing_matched_by_name": matched_by_name,
        "housing_missing_values": missing_profile(housing_district, HOUSING_FIELDS),
        "pension_rows_in_file": len(pension_rows),
        "pension_rows_filtered": len(pension_state),
        "pension_exact_duplicates_removed": pension_exact,
        "pension_conflicting_keys": len(pension_conflicts),
        "pension_rows_after_dedup": len(pension_kept),
        "pension_scheme_rows_published": len(pensions),
        "pension_grain": "state_scheme",
        "error_count": sum(1 for issue in issues if issue.severity == "error"),
        "warning_count": sum(1 for issue in issues if issue.severity == "warning"),
        "missing_value_treatment": (
            "Blank, null, and non-numeric tokens stay null. They are not replaced with zero."
        ),
    }
    return CleanOutput(districts, demographics, housing, pensions, issues, stats)
