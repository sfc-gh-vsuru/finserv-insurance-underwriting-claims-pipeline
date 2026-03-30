import os
import json
import random
import hashlib
from datetime import date, datetime, timedelta
from faker import Faker

fake = Faker()
random.seed()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "incremental_changes.sql")
MAX_IDS_FILE = os.path.join(OUTPUT_DIR, "max_ids.json")

TODAY = date.today()
NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_max_ids():
    defaults = {
        "customer_id": 5000,
        "policy_id": 10000,
        "claim_id": 4000,
        "payment_id": 7000,
        "factor_id": 20000,
        "decision_id": 10000,
    }
    if os.path.exists(MAX_IDS_FILE):
        with open(MAX_IDS_FILE) as f:
            saved = json.load(f)
        for k in defaults:
            if k in saved:
                defaults[k] = saved[k]
        print(f"  Loaded max IDs from {MAX_IDS_FILE}")
    else:
        print(f"  WARNING: {MAX_IDS_FILE} not found, using defaults. Run load_incremental.py --query-ids first.")
    return defaults

NEW_CUSTOMERS = 50
NEW_POLICIES = 80
NEW_CLAIMS = 30
NEW_CLAIM_PAYMENTS = 25

UPDATE_CLAIM_STATUSES = 40
UPDATE_POLICY_STATUSES = 30
UPDATE_CUSTOMER_ADDRESSES = 20
UPDATE_UNDERWRITING_DECISIONS = 15

DELETE_RISK_FACTORS = 25
DELETE_CANCELLED_POLICIES = 5

PRODUCT_TYPES = ["AUTO", "HOME", "LIFE", "HEALTH", "COMMERCIAL"]
PRODUCT_WEIGHTS = [0.35, 0.25, 0.15, 0.15, 0.10]
CLAIM_TYPES_BY_PRODUCT = {
    "AUTO": ["COLLISION", "THEFT", "LIABILITY"],
    "HOME": ["FIRE", "WATER", "THEFT", "PROPERTY"],
    "LIFE": ["MEDICAL"],
    "HEALTH": ["MEDICAL"],
    "COMMERCIAL": ["PROPERTY", "LIABILITY", "FIRE"],
}
PRIORITIES = ["LOW", "MEDIUM", "HIGH", "URGENT"]
PAYMENT_METHODS = ["CHECK", "ACH", "WIRE"]
STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
]
OCCUPATIONS = [
    "Software Engineer", "Teacher", "Nurse", "Accountant", "Sales Manager",
    "Electrician", "Attorney", "Physician", "Pharmacist", "Architect",
]
RISK_FACTORS_BY_PRODUCT = {
    "AUTO": [("DRIVING_RECORD", "Clean"), ("VEHICLE_AGE", "0-2 years"), ("ANNUAL_MILEAGE", "Under 5K")],
    "HOME": [("PROPERTY_AGE", "0-5 years"), ("ROOF_CONDITION", "Good"), ("FLOOD_ZONE", "None")],
    "LIFE": [("HEALTH_CONDITION", "Good"), ("SMOKER_STATUS", "Non-smoker")],
    "HEALTH": [("PRE_EXISTING", "None"), ("AGE_BAND", "31-45")],
    "COMMERCIAL": [("INDUSTRY_RISK", "Low (office)"), ("EMPLOYEE_COUNT", "11-50")],
}
CLAIM_DESCS = [
    "Rear-end collision at intersection during rush hour",
    "Water damage from burst pipe in basement",
    "Vehicle break-in at shopping center parking lot",
    "Kitchen fire triggered sprinkler system",
    "Slip and fall incident on icy walkway",
    "Storm damage to roof shingles and gutters",
    "Medical emergency requiring ambulance transport",
    "Vandalism to commercial storefront windows",
    "Fender bender in drive-through lane",
    "Tree limb fell on parked vehicle during windstorm",
]


def esc(val):
    if val is None:
        return "NULL"
    s = str(val).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def generate_sql():
    max_ids = load_max_ids()

    stmts = []
    stmts.append("-- =============================================================")
    stmts.append(f"-- Incremental CDC test data — generated {NOW}")
    stmts.append("-- Operations: INSERT, UPDATE, DELETE")
    stmts.append(f"-- Base IDs: {json.dumps(max_ids)}")
    stmts.append("-- =============================================================")
    stmts.append("USE insurance_db;")
    stmts.append("SET FOREIGN_KEY_CHECKS=0;")
    stmts.append("")

    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- INSERTS: New customers")
    stmts.append("-- -----------------------------------------------------------")
    cust_start_id = max_ids["customer_id"] + 1
    for i in range(NEW_CUSTOMERS):
        cid = cust_start_id + i
        dob = fake.date_of_birth(minimum_age=18, maximum_age=75).strftime("%Y-%m-%d")
        ssn_hash = hashlib.sha256(fake.ssn().encode()).hexdigest()
        gender = random.choice(["M", "F", "O"])
        credit = max(300, min(850, int(random.gauss(700, 80))))
        occ = random.choice(OCCUPATIONS)
        income = round(random.uniform(35000, 200000), 2)
        state = random.choice(STATES)
        stmts.append(
            f"INSERT INTO customers (customer_id, first_name, last_name, date_of_birth, gender, "
            f"ssn_hash, email, phone, address_line1, city, state, zip_code, credit_score, "
            f"occupation, annual_income) VALUES "
            f"({cid}, {esc(fake.first_name())}, {esc(fake.last_name())}, {esc(dob)}, {esc(gender)}, "
            f"{esc(ssn_hash)}, {esc(fake.email())}, {esc(fake.phone_number()[:20])}, "
            f"{esc(fake.street_address()[:255])}, {esc(fake.city())}, {esc(state)}, "
            f"{esc(fake.zipcode())}, {credit}, {esc(occ)}, {income});"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- INSERTS: New policies for new and existing customers")
    stmts.append("-- -----------------------------------------------------------")
    pol_start_id = max_ids["policy_id"] + 1
    new_policies = []
    for i in range(NEW_POLICIES):
        pid = pol_start_id + i
        cid = random.choice(list(range(cust_start_id, cust_start_id + NEW_CUSTOMERS)) + list(range(1, 100)))
        product = random.choices(PRODUCT_TYPES, weights=PRODUCT_WEIGHTS, k=1)[0]
        coverage = round(random.uniform(20000, 1000000), 2)
        premium = round(coverage * random.uniform(0.01, 0.08), 2)
        deductible = round(random.uniform(250, 5000), 2)
        eff = TODAY - timedelta(days=random.randint(0, 30))
        exp = eff + timedelta(days=365)
        risk = round(max(1, min(100, random.gauss(45, 20))), 2)
        pol_num = f"POL-{eff.year}-{pid:05d}"
        new_policies.append((pid, cid, product, coverage, premium, eff))
        stmts.append(
            f"INSERT INTO policies (policy_id, policy_number, customer_id, product_type, "
            f"coverage_amount, premium_amount, deductible, effective_date, expiration_date, "
            f"status, underwriting_status, risk_score) VALUES "
            f"({pid}, {esc(pol_num)}, {cid}, {esc(product)}, {coverage}, {premium}, {deductible}, "
            f"{esc(eff.strftime('%Y-%m-%d'))}, {esc(exp.strftime('%Y-%m-%d'))}, 'ACTIVE', 'PENDING', {risk});"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- INSERTS: Underwriting decisions for new policies")
    stmts.append("-- -----------------------------------------------------------")
    for pid, cid, product, coverage, premium, eff in new_policies:
        uw_id = random.randint(1, 25)
        decision = random.choices(["APPROVED", "DECLINED", "REFERRED", "COUNTER_OFFER"], weights=[0.65, 0.15, 0.12, 0.08], k=1)[0]
        risk_cat = random.choice(["LOW", "MEDIUM", "HIGH", "VERY_HIGH"])
        risk_score = round(random.uniform(10, 90), 2)
        adj_pct = round(random.uniform(-5, 15), 2)
        dec_date = (eff - timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d %H:%M:%S")
        review = 1 if risk_cat in ("HIGH", "VERY_HIGH") and random.random() < 0.4 else 0
        stmts.append(
            f"INSERT INTO underwriting_decisions (policy_id, underwriter_id, decision, risk_category, "
            f"risk_score, premium_adjustment_pct, decision_date, review_flag) VALUES "
            f"({pid}, {uw_id}, {esc(decision)}, {esc(risk_cat)}, {risk_score}, {adj_pct}, "
            f"{esc(dec_date)}, {review});"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- INSERTS: New claims (recent incidents)")
    stmts.append("-- -----------------------------------------------------------")
    claim_start_id = max_ids["claim_id"] + 1
    new_claims = []
    for i in range(NEW_CLAIMS):
        clm_id = claim_start_id + i
        pol = random.choice(new_policies[:20]) if new_policies else (10001, 1, "AUTO", 50000, 2000, TODAY)
        pid, cid, product = pol[0], pol[1], pol[2]
        claim_type = random.choice(CLAIM_TYPES_BY_PRODUCT.get(product, ["PROPERTY"]))
        incident = TODAY - timedelta(days=random.randint(0, 14))
        reported = incident + timedelta(days=random.randint(0, 3))
        estimated = round(random.uniform(500, 50000), 2)
        priority = random.choice(PRIORITIES)
        adj_id = random.randint(1, 30)
        fraud = 1 if random.random() < 0.08 else 0
        desc = random.choice(CLAIM_DESCS)
        clm_num = f"CLM-{reported.year}-{clm_id:05d}"
        new_claims.append((clm_id, pid, estimated))
        stmts.append(
            f"INSERT INTO claims (claim_id, claim_number, policy_id, customer_id, incident_date, "
            f"reported_date, claim_type, description, estimated_amount, status, priority, "
            f"adjuster_id, fraud_flag) VALUES "
            f"({clm_id}, {esc(clm_num)}, {pid}, {cid}, {esc(incident.strftime('%Y-%m-%d'))}, "
            f"{esc(reported.strftime('%Y-%m-%d'))}, {esc(claim_type)}, {esc(desc)}, {estimated}, "
            f"'OPEN', {esc(priority)}, {adj_id}, {fraud});"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- INSERTS: New claim payments")
    stmts.append("-- -----------------------------------------------------------")
    pay_start_id = max_ids["payment_id"] + 1
    for i in range(min(NEW_CLAIM_PAYMENTS, len(new_claims))):
        pay_id = pay_start_id + i
        clm_id, pid, estimated = new_claims[i]
        amount = round(estimated * random.uniform(0.3, 0.8), 2)
        pay_date = TODAY - timedelta(days=random.randint(0, 5))
        method = random.choice(PAYMENT_METHODS)
        stmts.append(
            f"INSERT INTO claim_payments (payment_id, claim_id, payment_date, amount, "
            f"payment_type, payment_method, payee_name) VALUES "
            f"({pay_id}, {clm_id}, {esc(pay_date.strftime('%Y-%m-%d'))}, {amount}, "
            f"'PARTIAL', {esc(method)}, {esc('Customer ' + str(random.randint(1, 5050)))});"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- INSERTS: New risk factors for new policies")
    stmts.append("-- -----------------------------------------------------------")
    rf_start_id = max_ids["factor_id"] + 1
    rf_count = 0
    for pid, cid, product, coverage, premium, eff in new_policies[:40]:
        factors = RISK_FACTORS_BY_PRODUCT.get(product, [])
        if not factors:
            continue
        ftype, fval = random.choice(factors)
        rf_count += 1
        impact = round(random.uniform(-10, 25), 2)
        assessed = (eff - timedelta(days=random.randint(1, 14))).strftime("%Y-%m-%d")
        stmts.append(
            f"INSERT INTO risk_factors (factor_id, policy_id, factor_type, factor_value, "
            f"impact_score, source, assessed_date) VALUES "
            f"({rf_start_id + rf_count}, {pid}, {esc(ftype)}, {esc(fval)}, {impact}, "
            f"'APPLICATION', {esc(assessed)});"
        )

    stmts.append("")
    stmts.append("-- ===========================================================")
    stmts.append("-- UPDATES: Claim status progressions")
    stmts.append("-- ===========================================================")
    transitions = [
        ("OPEN", "UNDER_REVIEW"),
        ("UNDER_REVIEW", "APPROVED"),
        ("APPROVED", "SETTLED"),
        ("OPEN", "DENIED"),
    ]
    for i in range(UPDATE_CLAIM_STATUSES):
        old_status, new_status = random.choice(transitions)
        claim_id = random.randint(1, 4000)
        approved_clause = ""
        if new_status == "APPROVED":
            approved_clause = f", approved_amount = estimated_amount * {round(random.uniform(0.6, 1.0), 2)}"
        elif new_status == "DENIED":
            approved_clause = ", approved_amount = 0"
        stmts.append(
            f"UPDATE claims SET status = {esc(new_status)}{approved_clause}, "
            f"updated_at = {esc(NOW)} WHERE claim_id = {claim_id} AND status = {esc(old_status)};"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- UPDATES: Policy status changes")
    stmts.append("-- -----------------------------------------------------------")
    for i in range(UPDATE_POLICY_STATUSES):
        pid = random.randint(1, 10000)
        new_status = random.choice(["EXPIRED", "CANCELLED", "SUSPENDED"])
        stmts.append(
            f"UPDATE policies SET status = {esc(new_status)}, "
            f"updated_at = {esc(NOW)} WHERE policy_id = {pid} AND status = 'ACTIVE';"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- UPDATES: Customer address changes")
    stmts.append("-- -----------------------------------------------------------")
    for i in range(UPDATE_CUSTOMER_ADDRESSES):
        cid = random.randint(1, 5000)
        new_addr = fake.street_address()[:255].replace("'", "\\'")
        new_city = fake.city().replace("'", "\\'")
        new_state = random.choice(STATES)
        new_zip = fake.zipcode()
        stmts.append(
            f"UPDATE customers SET address_line1 = {esc(new_addr)}, city = {esc(new_city)}, "
            f"state = {esc(new_state)}, zip_code = {esc(new_zip)}, "
            f"updated_at = {esc(NOW)} WHERE customer_id = {cid};"
        )

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- UPDATES: Underwriting decision overrides")
    stmts.append("-- -----------------------------------------------------------")
    for i in range(UPDATE_UNDERWRITING_DECISIONS):
        did = random.randint(1, 10000)
        new_decision = random.choice(["APPROVED", "COUNTER_OFFER"])
        new_adj = round(random.uniform(-3, 8), 2)
        date_str = TODAY.strftime("%Y-%m-%d")
        stmts.append(
            f"UPDATE underwriting_decisions SET decision = {esc(new_decision)}, "
            f"premium_adjustment_pct = {new_adj}, "
            f"notes = 'Revised on {date_str} -- incremental CDC test' "
            f"WHERE decision_id = {did};"
        )

    stmts.append("")
    stmts.append("-- ===========================================================")
    stmts.append("-- DELETES: Removed risk factors (reassessment)")
    stmts.append("-- ===========================================================")
    for i in range(DELETE_RISK_FACTORS):
        fid = random.randint(1, 20000)
        stmts.append(f"DELETE FROM risk_factors WHERE factor_id = {fid};")

    stmts.append("")
    stmts.append("-- -----------------------------------------------------------")
    stmts.append("-- DELETES: Cancelled policies with no claims (cleanup)")
    stmts.append("-- -----------------------------------------------------------")
    for i in range(DELETE_CANCELLED_POLICIES):
        pid = random.randint(8000, 10000)
        stmts.append(
            f"DELETE FROM risk_factors WHERE policy_id = {pid};"
        )
        stmts.append(
            f"DELETE FROM underwriting_decisions WHERE policy_id = {pid};"
        )
        stmts.append(
            f"DELETE FROM policies WHERE policy_id = {pid} "
            f"AND status = 'CANCELLED' "
            f"AND policy_id NOT IN (SELECT policy_id FROM claims);"
        )

    stmts.append("")
    stmts.append("SET FOREIGN_KEY_CHECKS=1;")
    stmts.append("")
    stmts.append(f"-- Summary: {NEW_CUSTOMERS} new customers, {NEW_POLICIES} new policies,")
    stmts.append(f"-- {NEW_CLAIMS} new claims, {NEW_CLAIM_PAYMENTS} new payments,")
    stmts.append(f"-- {rf_count} new risk factors,")
    stmts.append(f"-- {UPDATE_CLAIM_STATUSES} claim status updates, {UPDATE_POLICY_STATUSES} policy status updates,")
    stmts.append(f"-- {UPDATE_CUSTOMER_ADDRESSES} address updates, {UPDATE_UNDERWRITING_DECISIONS} decision overrides,")
    stmts.append(f"-- {DELETE_RISK_FACTORS} risk factor deletes, {DELETE_CANCELLED_POLICIES} policy cascade deletes")

    with open(OUTPUT_FILE, "w") as f:
        f.write("\n".join(stmts))

    total_inserts = NEW_CUSTOMERS + NEW_POLICIES + NEW_POLICIES + NEW_CLAIMS + NEW_CLAIM_PAYMENTS + rf_count
    total_updates = UPDATE_CLAIM_STATUSES + UPDATE_POLICY_STATUSES + UPDATE_CUSTOMER_ADDRESSES + UPDATE_UNDERWRITING_DECISIONS
    total_deletes = DELETE_RISK_FACTORS + DELETE_CANCELLED_POLICIES * 3

    print(f"Generated incremental SQL: {OUTPUT_FILE}")
    print(f"  INSERTs: ~{total_inserts} (customers, policies, decisions, claims, payments, risk factors)")
    print(f"  UPDATEs: ~{total_updates} (claim statuses, policy statuses, addresses, decisions)")
    print(f"  DELETEs: ~{total_deletes} (risk factors, cancelled policy cascades)")
    print(f"\nNext: Run load_incremental.py to execute on MySQL")


if __name__ == "__main__":
    generate_sql()
