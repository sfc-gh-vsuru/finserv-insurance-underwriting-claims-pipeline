import os
import csv
import random
import hashlib
from datetime import date, datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS = 5000
NUM_POLICIES = 10000
NUM_CLAIMS = 4000
NUM_CLAIM_PAYMENTS = 6000
NUM_UNDERWRITERS = 25
NUM_ADJUSTERS = 30
NUM_RISK_FACTORS = 20000

PRODUCT_TYPES = ["AUTO", "HOME", "LIFE", "HEALTH", "COMMERCIAL"]
PRODUCT_WEIGHTS = [0.35, 0.25, 0.15, 0.15, 0.10]

POLICY_STATUSES = ["ACTIVE", "EXPIRED", "CANCELLED", "SUSPENDED"]
POLICY_STATUS_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

UW_STATUSES = ["APPROVED", "DECLINED", "REFERRED", "PENDING"]
UW_STATUS_WEIGHTS = [0.68, 0.12, 0.10, 0.10]

DECISIONS = ["APPROVED", "DECLINED", "REFERRED", "COUNTER_OFFER"]
DECISION_WEIGHTS = [0.65, 0.15, 0.12, 0.08]

RISK_CATEGORIES = ["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]

CLAIM_TYPES_BY_PRODUCT = {
    "AUTO": ["COLLISION", "THEFT", "LIABILITY"],
    "HOME": ["FIRE", "WATER", "THEFT", "PROPERTY"],
    "LIFE": ["MEDICAL"],
    "HEALTH": ["MEDICAL"],
    "COMMERCIAL": ["PROPERTY", "LIABILITY", "FIRE"],
}

CLAIM_STATUSES = ["OPEN", "UNDER_REVIEW", "APPROVED", "DENIED", "SETTLED", "CLOSED"]
CLAIM_STATUS_WEIGHTS = [0.15, 0.10, 0.10, 0.05, 0.40, 0.20]

PRIORITIES = ["LOW", "MEDIUM", "HIGH", "URGENT"]
PRIORITY_WEIGHTS = [0.20, 0.45, 0.25, 0.10]

PAYMENT_TYPES = ["PARTIAL", "FINAL", "SUPPLEMENT"]
PAYMENT_METHODS = ["CHECK", "ACH", "WIRE"]

UW_SPECIALIZATIONS = ["AUTO", "HOME", "LIFE", "HEALTH", "COMMERCIAL", "GENERAL"]
ADJ_SPECIALIZATIONS = ["AUTO", "PROPERTY", "LIABILITY", "MEDICAL"]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West", "Pacific Northwest"]

OCCUPATIONS = [
    "Software Engineer", "Teacher", "Nurse", "Accountant", "Sales Manager",
    "Electrician", "Attorney", "Physician", "Pharmacist", "Architect",
    "Chef", "Mechanic", "Truck Driver", "Dentist", "Real Estate Agent",
    "Marketing Manager", "Financial Analyst", "Civil Engineer", "Police Officer",
    "Firefighter", "Pilot", "Veterinarian", "Plumber", "Consultant", "Retired",
]

RISK_FACTORS_BY_PRODUCT = {
    "AUTO": [
        ("DRIVING_RECORD", ["Clean", "1 violation", "2 violations", "3+ violations", "DUI"], "DMV"),
        ("VEHICLE_AGE", ["0-2 years", "3-5 years", "6-10 years", "10+ years"], "REGISTRATION"),
        ("ANNUAL_MILEAGE", ["Under 5K", "5K-10K", "10K-15K", "15K-25K", "25K+"], "SELF_REPORTED"),
        ("DRIVER_AGE_GROUP", ["16-25", "26-35", "36-50", "51-65", "65+"], "APPLICATION"),
    ],
    "HOME": [
        ("PROPERTY_AGE", ["0-5 years", "6-15 years", "16-30 years", "30+ years"], "INSPECTION"),
        ("ROOF_CONDITION", ["Excellent", "Good", "Fair", "Poor"], "INSPECTION"),
        ("FLOOD_ZONE", ["None", "Moderate", "High"], "FEMA"),
        ("SECURITY_SYSTEM", ["Full system", "Partial", "None"], "APPLICATION"),
    ],
    "LIFE": [
        ("HEALTH_CONDITION", ["Excellent", "Good", "Fair", "Poor"], "MEDICAL"),
        ("SMOKER_STATUS", ["Non-smoker", "Former smoker", "Current smoker"], "APPLICATION"),
        ("BMI_RANGE", ["Normal", "Overweight", "Obese"], "MEDICAL"),
        ("FAMILY_HISTORY", ["No conditions", "1 condition", "2+ conditions"], "APPLICATION"),
    ],
    "HEALTH": [
        ("PRE_EXISTING", ["None", "1 condition", "2 conditions", "3+ conditions"], "MEDICAL"),
        ("AGE_BAND", ["18-30", "31-45", "46-60", "60+"], "APPLICATION"),
        ("LIFESTYLE", ["Active", "Moderate", "Sedentary"], "SELF_REPORTED"),
        ("PRESCRIPTION_COUNT", ["0", "1-3", "4-6", "7+"], "PHARMACY"),
    ],
    "COMMERCIAL": [
        ("INDUSTRY_RISK", ["Low (office)", "Medium (retail)", "High (manufacturing)", "Very High (construction)"], "UNDERWRITER"),
        ("EMPLOYEE_COUNT", ["1-10", "11-50", "51-200", "200+"], "APPLICATION"),
        ("CLAIMS_HISTORY", ["No prior claims", "1-2 claims", "3+ claims"], "INTERNAL"),
        ("LOCATION_RISK", ["Low crime area", "Medium crime area", "High crime area"], "THIRD_PARTY"),
    ],
}

IMPACT_SCORES_BY_POSITION = {
    0: (-15, -5),
    1: (-5, 5),
    2: (5, 15),
    3: (15, 30),
    4: (25, 40),
}

COVERAGE_RANGES = {
    "AUTO":       (15000, 100000),
    "HOME":       (150000, 750000),
    "LIFE":       (100000, 2000000),
    "HEALTH":     (50000, 500000),
    "COMMERCIAL": (500000, 5000000),
}

PREMIUM_RATE = {
    "AUTO":       (0.03, 0.08),
    "HOME":       (0.003, 0.012),
    "LIFE":       (0.005, 0.025),
    "HEALTH":     (0.04, 0.12),
    "COMMERCIAL": (0.01, 0.04),
}

DEDUCTIBLE_RANGES = {
    "AUTO":       (250, 2000),
    "HOME":       (500, 5000),
    "LIFE":       (0, 0),
    "HEALTH":     (500, 10000),
    "COMMERCIAL": (1000, 25000),
}

START_DATE = date(2023, 1, 1)
END_DATE = date(2026, 3, 26)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, max(delta.days, 0)))


def write_csv(filename, rows, headers):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  {filename}: {len(rows)} rows")
    return path


def generate_underwriters():
    rows = []
    for i in range(1, NUM_UNDERWRITERS + 1):
        rows.append([
            i,
            fake.first_name(),
            fake.last_name(),
            f"UW-{i:04d}",
            random.choice(UW_SPECIALIZATIONS),
            random.randint(1, 30),
            round(random.uniform(500000, 5000000), 2),
            1,
            fake.date_time_between(start_date="-5y", end_date="-1y").strftime("%Y-%m-%d %H:%M:%S"),
        ])
    return write_csv("underwriters.csv", rows, [
        "underwriter_id", "first_name", "last_name", "employee_id",
        "specialization", "experience_years", "approval_limit", "active", "created_at",
    ])


def generate_adjusters():
    rows = []
    for i in range(1, NUM_ADJUSTERS + 1):
        rows.append([
            i,
            fake.first_name(),
            fake.last_name(),
            f"ADJ-{i:04d}",
            random.choice(REGIONS),
            random.choice(ADJ_SPECIALIZATIONS),
            1,
            fake.date_time_between(start_date="-5y", end_date="-1y").strftime("%Y-%m-%d %H:%M:%S"),
        ])
    return write_csv("adjusters.csv", rows, [
        "adjuster_id", "first_name", "last_name", "employee_id",
        "region", "specialization", "active", "created_at",
    ])


def generate_customers():
    rows = []
    states = [
        "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
        "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
        "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
    ]
    for i in range(1, NUM_CUSTOMERS + 1):
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        ssn_hash = hashlib.sha256(fake.ssn().encode()).hexdigest()
        credit = int(random.gauss(700, 80))
        credit = max(300, min(850, credit))
        occupation = random.choice(OCCUPATIONS)
        income_base = {
            "Physician": 250000, "Attorney": 180000, "Dentist": 200000,
            "Pilot": 160000, "Pharmacist": 130000, "Software Engineer": 140000,
            "Architect": 110000, "Financial Analyst": 100000, "Civil Engineer": 95000,
            "Consultant": 120000, "Marketing Manager": 90000, "Sales Manager": 85000,
            "Accountant": 75000, "Nurse": 70000, "Real Estate Agent": 80000,
            "Teacher": 55000, "Police Officer": 65000, "Firefighter": 62000,
            "Electrician": 60000, "Plumber": 58000, "Chef": 45000,
            "Mechanic": 50000, "Truck Driver": 55000, "Veterinarian": 100000,
            "Retired": 40000,
        }.get(occupation, 60000)
        income = round(income_base * random.uniform(0.7, 1.5), 2)
        created = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        rows.append([
            i,
            fake.first_name(),
            fake.last_name(),
            dob.strftime("%Y-%m-%d"),
            random.choice(["M", "F", "O"]),
            ssn_hash,
            fake.email(),
            fake.phone_number()[:20],
            fake.street_address()[:255],
            fake.city(),
            random.choice(states),
            fake.zipcode(),
            credit,
            occupation,
            income,
            created.strftime("%Y-%m-%d %H:%M:%S"),
            created.strftime("%Y-%m-%d %H:%M:%S"),
        ])
    return write_csv("customers.csv", rows, [
        "customer_id", "first_name", "last_name", "date_of_birth", "gender",
        "ssn_hash", "email", "phone", "address_line1", "city", "state",
        "zip_code", "credit_score", "occupation", "annual_income",
        "created_at", "updated_at",
    ]), rows


def generate_policies(customers):
    rows = []
    customer_ids = [c[0] for c in customers]

    for i in range(1, NUM_POLICIES + 1):
        cust_id = random.choice(customer_ids)
        product = random.choices(PRODUCT_TYPES, weights=PRODUCT_WEIGHTS, k=1)[0]

        cov_min, cov_max = COVERAGE_RANGES[product]
        coverage = round(random.uniform(cov_min, cov_max), 2)

        rate_min, rate_max = PREMIUM_RATE[product]
        premium = round(coverage * random.uniform(rate_min, rate_max), 2)

        ded_min, ded_max = DEDUCTIBLE_RANGES[product]
        deductible = round(random.uniform(ded_min, ded_max), 2) if ded_max > 0 else 0

        eff_date = random_date(START_DATE, END_DATE - timedelta(days=365))
        exp_date = eff_date + timedelta(days=365)

        status = random.choices(POLICY_STATUSES, weights=POLICY_STATUS_WEIGHTS, k=1)[0]
        uw_status = random.choices(UW_STATUSES, weights=UW_STATUS_WEIGHTS, k=1)[0]

        risk = round(random.gauss(45, 20), 2)
        risk = max(1, min(100, risk))

        created = datetime.combine(eff_date, datetime.min.time()) + timedelta(hours=random.randint(8, 17))

        rows.append([
            i,
            f"POL-{eff_date.year}-{i:05d}",
            cust_id,
            product,
            coverage,
            premium,
            deductible,
            eff_date.strftime("%Y-%m-%d"),
            exp_date.strftime("%Y-%m-%d"),
            status,
            uw_status,
            risk,
            created.strftime("%Y-%m-%d %H:%M:%S"),
            created.strftime("%Y-%m-%d %H:%M:%S"),
        ])
    return write_csv("policies.csv", rows, [
        "policy_id", "policy_number", "customer_id", "product_type",
        "coverage_amount", "premium_amount", "deductible", "effective_date",
        "expiration_date", "status", "underwriting_status", "risk_score",
        "created_at", "updated_at",
    ]), rows


def generate_underwriting_decisions(policies):
    rows = []
    for i, pol in enumerate(policies, 1):
        policy_id = pol[0]
        uw_id = random.randint(1, NUM_UNDERWRITERS)
        decision = random.choices(DECISIONS, weights=DECISION_WEIGHTS, k=1)[0]
        risk_score = float(pol[11])

        if risk_score < 25:
            risk_cat = "LOW"
        elif risk_score < 50:
            risk_cat = "MEDIUM"
        elif risk_score < 75:
            risk_cat = "HIGH"
        else:
            risk_cat = "VERY_HIGH"

        adj_pct = round(random.uniform(-10, 25) if decision != "APPROVED" else random.uniform(-5, 10), 2)
        review = 1 if risk_cat in ("HIGH", "VERY_HIGH") and random.random() < 0.4 else 0

        eff_date = datetime.strptime(pol[7], "%Y-%m-%d")
        decision_date = eff_date - timedelta(days=random.randint(1, 14))
        decision_dt = datetime.combine(decision_date, datetime.min.time()) + timedelta(
            hours=random.randint(8, 17), minutes=random.randint(0, 59)
        )

        conditions_opts = [
            "", "", "",
            "Excluded pre-existing conditions",
            "Higher deductible required",
            "Annual inspection required",
            "Driver training course required",
            "Security system installation required",
            "Medical exam required within 30 days",
        ]
        notes_opts = [
            "", "", "",
            "Standard risk profile",
            "Borderline case, approved with conditions",
            "Referred to senior underwriter for review",
            "High risk score, additional documentation needed",
            "Competitive rate offered",
        ]

        rows.append([
            i,
            policy_id,
            uw_id,
            decision,
            risk_cat,
            risk_score,
            adj_pct,
            random.choice(conditions_opts),
            random.choice(notes_opts),
            decision_dt.strftime("%Y-%m-%d %H:%M:%S"),
            review,
            decision_dt.strftime("%Y-%m-%d %H:%M:%S"),
        ])
    return write_csv("underwriting_decisions.csv", rows, [
        "decision_id", "policy_id", "underwriter_id", "decision", "risk_category",
        "risk_score", "premium_adjustment_pct", "conditions", "notes",
        "decision_date", "review_flag", "created_at",
    ]), rows


def generate_claims(policies):
    rows = []
    eligible = [p for p in policies if p[9] in ("ACTIVE", "EXPIRED")]
    sampled = random.sample(eligible, min(NUM_CLAIMS, len(eligible)))

    claim_descs = {
        "COLLISION": [
            "Rear-end collision at intersection",
            "Side-impact crash on highway",
            "Parking lot fender bender",
            "Multi-vehicle pileup on interstate",
            "Single-car accident in adverse weather",
        ],
        "THEFT": [
            "Vehicle stolen from parking garage",
            "Break-in with electronics stolen",
            "Home burglary while on vacation",
            "Catalytic converter theft",
            "Package theft from doorstep",
        ],
        "FIRE": [
            "Kitchen fire from unattended cooking",
            "Electrical fire in attic wiring",
            "Wildfire damage to exterior",
            "Garage fire from chemical storage",
            "Heating system malfunction",
        ],
        "WATER": [
            "Burst pipe during winter freeze",
            "Roof leak from storm damage",
            "Washing machine overflow",
            "Sump pump failure during heavy rain",
            "Hot water heater burst",
        ],
        "LIABILITY": [
            "Slip and fall on property",
            "Dog bite incident on premises",
            "At-fault accident with injuries",
            "Property damage to neighbor",
            "Product liability claim",
        ],
        "MEDICAL": [
            "Emergency room visit after accident",
            "Surgery required for injuries",
            "Ongoing physical therapy treatment",
            "Specialist consultation and tests",
            "Hospitalization for acute condition",
        ],
        "PROPERTY": [
            "Storm damage to roof and siding",
            "Tree fell on structure",
            "Vandalism to commercial property",
            "Foundation damage from settling",
            "HVAC system failure and replacement",
        ],
    }

    for i, pol in enumerate(sampled, 1):
        policy_id = pol[0]
        cust_id = pol[2]
        product = pol[3]

        claim_types = CLAIM_TYPES_BY_PRODUCT.get(product, ["PROPERTY"])
        claim_type = random.choice(claim_types)

        eff = datetime.strptime(pol[7], "%Y-%m-%d").date()
        exp = datetime.strptime(pol[8], "%Y-%m-%d").date()
        latest = min(exp, END_DATE)
        incident = random_date(eff, latest)
        report_delay = random.choices([0, 1, 2, 3, 5, 7, 14, 30], weights=[0.1, 0.3, 0.2, 0.15, 0.1, 0.08, 0.05, 0.02], k=1)[0]
        reported = min(incident + timedelta(days=report_delay), END_DATE)

        coverage = float(pol[4])
        est_pct = random.uniform(0.02, 0.6)
        estimated = round(coverage * est_pct, 2)

        status = random.choices(CLAIM_STATUSES, weights=CLAIM_STATUS_WEIGHTS, k=1)[0]

        if status in ("APPROVED", "SETTLED", "CLOSED"):
            approved = round(estimated * random.uniform(0.5, 1.1), 2)
        elif status == "DENIED":
            approved = 0
        else:
            approved = None

        priority = random.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0]
        adj_id = random.randint(1, NUM_ADJUSTERS)
        fraud = 1 if random.random() < 0.05 else 0

        desc = random.choice(claim_descs.get(claim_type, ["Claim filed"]))
        created = datetime.combine(reported, datetime.min.time()) + timedelta(hours=random.randint(8, 17))

        rows.append([
            i,
            f"CLM-{reported.year}-{i:05d}",
            policy_id,
            cust_id,
            incident.strftime("%Y-%m-%d"),
            reported.strftime("%Y-%m-%d"),
            claim_type,
            desc,
            estimated,
            approved if approved is not None else "\\N",
            status,
            priority,
            adj_id,
            fraud,
            created.strftime("%Y-%m-%d %H:%M:%S"),
            created.strftime("%Y-%m-%d %H:%M:%S"),
        ])
    return write_csv("claims.csv", rows, [
        "claim_id", "claim_number", "policy_id", "customer_id", "incident_date",
        "reported_date", "claim_type", "description", "estimated_amount",
        "approved_amount", "status", "priority", "adjuster_id", "fraud_flag",
        "created_at", "updated_at",
    ]), rows


def generate_claim_payments(claims):
    rows = []
    payable = [c for c in claims if c[10] in ("APPROVED", "SETTLED", "CLOSED") and c[9] != "\\N"]

    payment_id = 0
    for claim in payable:
        claim_id = claim[0]
        approved = float(claim[9])
        if approved <= 0:
            continue

        reported = datetime.strptime(claim[5], "%Y-%m-%d").date()
        num_payments = random.choices([1, 2, 3], weights=[0.5, 0.35, 0.15], k=1)[0]

        remaining = approved
        for j in range(num_payments):
            payment_id += 1
            pay_date = reported + timedelta(days=random.randint(7 + j * 30, 30 + j * 45))
            pay_date = min(pay_date, END_DATE)

            if j == num_payments - 1:
                amount = round(remaining, 2)
                ptype = "FINAL"
            else:
                amount = round(remaining * random.uniform(0.3, 0.6), 2)
                ptype = "PARTIAL" if j == 0 else "SUPPLEMENT"
            remaining -= amount

            payee = f"{claim[3]}"
            created = datetime.combine(pay_date, datetime.min.time()) + timedelta(hours=random.randint(9, 16))

            rows.append([
                payment_id,
                claim_id,
                pay_date.strftime("%Y-%m-%d"),
                amount,
                ptype,
                random.choice(PAYMENT_METHODS),
                f"Customer {payee}",
                "",
                created.strftime("%Y-%m-%d %H:%M:%S"),
            ])

            if payment_id >= NUM_CLAIM_PAYMENTS:
                break
        if payment_id >= NUM_CLAIM_PAYMENTS:
            break

    return write_csv("claim_payments.csv", rows, [
        "payment_id", "claim_id", "payment_date", "amount", "payment_type",
        "payment_method", "payee_name", "notes", "created_at",
    ]), rows


def generate_risk_factors(policies):
    rows = []
    factor_id = 0

    for pol in policies:
        policy_id = pol[0]
        product = pol[3]
        eff_date = datetime.strptime(pol[7], "%Y-%m-%d").date()

        factors_for_product = RISK_FACTORS_BY_PRODUCT.get(product, [])
        num = random.randint(1, min(3, len(factors_for_product)))
        chosen = random.sample(factors_for_product, num)

        for factor_type, values, source in chosen:
            factor_id += 1
            idx = random.randint(0, len(values) - 1)
            value = values[idx]

            score_min, score_max = IMPACT_SCORES_BY_POSITION.get(idx, (0, 10))
            impact = round(random.uniform(score_min, score_max), 2)

            assessed = eff_date - timedelta(days=random.randint(1, 30))
            created = datetime.combine(assessed, datetime.min.time()) + timedelta(hours=random.randint(8, 17))

            rows.append([
                factor_id,
                policy_id,
                factor_type,
                value,
                impact,
                source,
                assessed.strftime("%Y-%m-%d"),
                created.strftime("%Y-%m-%d %H:%M:%S"),
            ])

            if factor_id >= NUM_RISK_FACTORS:
                break
        if factor_id >= NUM_RISK_FACTORS:
            break

    return write_csv("risk_factors.csv", rows, [
        "factor_id", "policy_id", "factor_type", "factor_value",
        "impact_score", "source", "assessed_date", "created_at",
    ]), rows


def main():
    print("Generating mock insurance data...")
    print(f"Output directory: {OUTPUT_DIR}\n")

    print("[1/7] Underwriters")
    generate_underwriters()

    print("[2/7] Adjusters")
    generate_adjusters()

    print("[3/7] Customers")
    _, customers = generate_customers()

    print("[4/7] Policies")
    _, policies = generate_policies(customers)

    print("[5/7] Underwriting Decisions")
    generate_underwriting_decisions(policies)

    print("[6/7] Claims")
    _, claims = generate_claims(policies)

    print("[7/7] Claim Payments")
    generate_claim_payments(claims)

    print("\n[Bonus] Risk Factors")
    generate_risk_factors(policies)

    print(f"\nDone! CSV files written to {OUTPUT_DIR}/")
    print("Next: Run load_to_mysql.py to load into EC2 MySQL instance.")


if __name__ == "__main__":
    main()
