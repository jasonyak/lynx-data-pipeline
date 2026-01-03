
import json
import datetime
import argparse
import random
import re
from typing import Dict, Any, List, Optional


class Standardizer:
    def __init__(self):
        pass

    def _get_formatted_record(self, record: Dict[str, Any]) -> str:
        """
        Returns a LLM-friendly key-value text representation of the record.
        """
        lines = []
        for k, v in record.items():
            val_str = str(v).strip()
            if val_str:
                lines.append(f"{k}: {val_str}")
        return "\n".join(lines)

    def _normalize_name(self, name: Optional[str]) -> str:
        if not name:
            return "Unknown"
        # Basic title casing and whitespace stripping
        return " ".join(name.split()).title()

    def _normalize_status(self, status_raw: str, source: str) -> str:
        if not status_raw:
            return "Unknown"
        
        s = status_raw.lower()
        if source == "TX":
            if s == "y": return "Active"
            if s == "n": return "Inactive"
        elif source == "WA":
            if "not active" in s: return "Inactive"
            if "active" in s: return "Active"
            
        return "Unknown"

    def _normalize_type(self, type_raw: str) -> str:
        if not type_raw:
            return "Other"
        
        t = type_raw.lower()
        if "child placing" in t or "agency" in t or "placement" in t:
            return "Agency"
        if "residential" in t or "treatment" in t or "shelter" in t:
            return "Residential"
        if any(x in t for x in ["center"]):
            return "Center"
        if any(x in t for x in ["home", "family"]):
            return "Home"
        if "school" in t:
            return "School"
        
        return "Other"

    def _normalize_date(self, date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            return None
        # Attempt minimal parsing for known formats
        # TX: YYYY-MM-DD...
        # WA: M/D/YYYY
        try:
            if "T" in date_str:
                return date_str.split("T")[0]
            if "/" in date_str:
                parts = date_str.split("/")
                if len(parts) == 3:
                     # M/D/YYYY -> YYYY-MM-DD
                    return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        except Exception:
            pass
        return date_str # Fallback to original if parsing fails provided it's string-ish
        # Attempt minimal parsing for known formats
        # TX: YYYY-MM-DD...
        # WA: M/D/YYYY
        try:
            if "T" in date_str:
                return date_str.split("T")[0]
            if "/" in date_str:
                parts = date_str.split("/")
                if len(parts) == 3:
                     # M/D/YYYY -> YYYY-MM-DD
                    return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        except Exception:
            pass
        return date_str # Fallback to original if parsing fails provided it's string-ish

    def _normalize_phone(self, phone_raw: Optional[str]) -> Optional[str]:
        if not phone_raw:
            return None
        
        # Remove all non-digit characters
        digits = re.sub(r"\D", "", str(phone_raw))
        
        # Handle leading 1 (US country code)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
            
        if len(digits) == 10:
            return digits
            
        return None

    def _normalize_email(self, email_raw: Optional[str]) -> Optional[str]:
        if not email_raw:
            return None
            
        email = str(email_raw).strip().lower()
        if "@" in email and "." in email:
            return email
            
        return None

    def standardize_tx(self, record: Dict[str, Any]) -> Dict[str, Any]:
        mapped = {}
        
        # Identity
        mapped["id"] = f"TX-{record.get('operation_id', 'UNKNOWN')}"
        mapped["source_state"] = "TX"
        mapped["original_record"] = self._get_formatted_record(record)
        
        mapped["name"] = self._normalize_name(record.get("operation_name"))
        mapped["type"] = self._normalize_type(record.get("operation_type", ""))
        mapped["status"] = self._normalize_status(record.get("operation_status", ""), "TX")
        # Check for temporary closure override
        if record.get("temporarily_closed") == "YES":
             mapped["status"] = "Temporarily Closed"
             
        mapped["license_date"] = self._normalize_date(record.get("issuance_date"))
        
        # Location
        mapped["address"] = {
            "full": record.get("location_address", ""),
            "street": record.get("address_line", ""),
            "city": record.get("city", ""),
            "state": record.get("state", ""),
            "zip": record.get("zipcode", ""),
            "latitude": float(record.get("location_address_geo", {}).get("latitude") or 0.0),
            "longitude": float(record.get("location_address_geo", {}).get("longitude") or 0.0)
        }
        
        # Contact
        mapped["contact"] = {
            "phone": self._normalize_phone(record.get("phone_number")),
            "email": self._normalize_email(record.get("email_address")),
            "website": record.get("website_address"),
            "director_name": record.get("administrator_director_name")
        }
        
        # Capacity
        cap = record.get("total_capacity")
        mapped["capacity"] = int(cap) if cap and str(cap).isdigit() else None
        mapped["ages_served"] = record.get("licensed_to_serve_ages")
        mapped["schedule"] = {
            "hours": record.get("hours_of_operation"),
            "days": record.get("days_of_operation")
        }
        
        return mapped

    def standardize_wa(self, record: Dict[str, Any]) -> Dict[str, Any]:
        mapped = {}
        
        # Identity
        mapped["id"] = f"WA-{record.get('wacompassid', 'UNKNOWN')}"
        mapped["source_state"] = "WA"
        mapped["original_record"] = self._get_formatted_record(record)
        
        # Name preference: DBA > ProviderName
        raw_name = record.get("doingbusinessas") or record.get("providername")
        mapped["name"] = self._normalize_name(raw_name)
        
        mapped["type"] = self._normalize_type(record.get("facilitytypegeneric", ""))
        mapped["status"] = self._normalize_status(record.get("latestoperatingstatus", ""), "WA")
        mapped["license_date"] = self._normalize_date(record.get("initiallicensedate"))
        
        # Location
        mapped["address"] = {
            "full": f"{record.get('physicalstreetaddress', '')}, {record.get('physicalcity', '')}, {record.get('physicalstate', '')} {record.get('physicalzip', '')}",
            "street": record.get("physicalstreetaddress", ""),
            "city": record.get("physicalcity", ""),
            "state": record.get("physicalstate", ""),
            "zip": record.get("physicalzip", ""),
            # Fix typo 'physciallatitude'
            "latitude": float(record.get("physciallatitude") or 0.0),
            "longitude": float(record.get("physicallongitude") or 0.0)
        }
        
        # Contact
        mapped["contact"] = {
            "phone": self._normalize_phone(record.get("primarycontactphonenumber")),
            "email": self._normalize_email(record.get("primarycontactemail")),
            "website": None, # Not present in sample
            "director_name": record.get("primarycontactpersonname")
        }
        
        # Capacity
        cap = record.get("licensecapacity")
        mapped["capacity"] = int(cap) if cap and str(cap).isdigit() else None
        
        start_age = record.get("startingage", "")
        end_age = record.get("endingage", "")
        mapped["ages_served"] = f"{start_age} - {end_age}" if start_age or end_age else None
        
        mapped["schedule"] = {
            "hours": None,
            "days": None
        }
        
        return mapped

def main():
    parser = argparse.ArgumentParser(description="Unify state daycare data to JSONL")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of records per state for testing")
    parser.add_argument("--random", action="store_true", help="Randomly sample records if limit is set")
    parser.add_argument("--city", type=str, default=None, help="Filter by city name (case-insensitive)")
    parser.add_argument("--state", type=str, default=None, help="Filter by state code (e.g. TX, WA)")
    args = parser.parse_args()
    
    standardizer = Standardizer()
    
    output_path = "data/unified_daycares.jsonl"
    print(f"Starting unified processing... writing to {output_path}")
    
    # Inputs assumed to be at project root / data (relative to execution from record-flow dir which is 2 levels deep from lynx root)
    # /Users/jason/workspace1/lynx/data -> ../../data
    sources = [
        {"path": "../../data/texas.json", "handler": standardizer.standardize_tx, "name": "Texas", "code": "TX"},
        {"path": "../../data/washington.json", "handler": standardizer.standardize_wa, "name": "Washington", "code": "WA"}
    ]
    
    seen_entries = set()

    drop_counts = {
        "missing_name": 0,
        "missing_address": 0,
        "inactive": 0,
        "duplicate": 0,
        "filtered_type": 0,
        "filtered_keyword": 0,
        "filtered_capacity": 0,
        "filtered_contact": 0,
        "filtered_city": 0,
        "filtered_state": 0
    }
    
    with open(output_path, "w") as outfile:
        for source in sources:
            # Pre-filter source if state flag is set
            if args.state and args.state.upper() != source["code"]:
                print(f"Skipping {source['name']} (State filter: {args.state})")
                continue

            print(f"Processing {source['name']}...")
            try:
                with open(source["path"], "r") as f:
                    data = json.load(f)
                
                print(f"  Found {len(data)} records in input file.")

                if args.limit:
                    if args.random:
                        print(f"  Randomly sampling {args.limit} records...")
                        sample_size = min(args.limit, len(data))
                        data = random.sample(data, sample_size)
                    else:
                        data = data[:args.limit]
                    
                count = 0
                for item in data:
                        
                    try:
                        unified = source["handler"](item)
                        
                        # 0. Heuristic Filters (Pre-check)
                        
                        # Filter A: Invalid Types
                        if unified.get("type") in ["Agency", "Residential"]:
                            drop_counts["filtered_type"] += 1
                            continue
                            
                        # Filter B: Keywords (Name Only - safer than raw record)
                        name = unified.get("name", "").lower()
                        exclude_keywords = [
                            "child placing", "residential treatment", 
                            "placement agency", "adoption", "foster care"
                        ]
                        if any(k in name for k in exclude_keywords):
                            drop_counts["filtered_keyword"] += 1
                            continue
                            
                        # Filter C: Capacity (must be > 0 if present)
                        cap = unified.get("capacity")
                        if cap is not None and cap == 0:
                            drop_counts["filtered_capacity"] += 1
                            continue
                            
                        # Filter D: Contact Info (Must have at least ONE contact method)
                        contact = unified.get("contact", {})
                        if not any([contact.get("phone"), contact.get("email"), contact.get("website")]):
                            drop_counts["filtered_contact"] += 1
                            continue

                        # 1. Check Name
                        if unified.get("name") == "Unknown" or not unified.get("name"):
                            drop_counts["missing_name"] += 1
                            continue

                        # 2. Check Address
                        address_obj = unified.get("address", {})
                        full_address = address_obj.get("full", "")
                        if not full_address:
                            drop_counts["missing_address"] += 1
                            continue
                            
                        # Filter: City and State
                        if args.city:
                            record_city = address_obj.get("city", "").lower().strip()
                            target_city = args.city.lower().strip()
                            if record_city != target_city:
                                drop_counts["filtered_city"] += 1
                                continue
                        
                        if args.state:
                            record_state = address_obj.get("state", "").upper().strip()
                            target_state = args.state.upper().strip()
                            # Standardize state codes just in case (e.g. "Texas" vs "TX")
                            # But assuming our scrapers are good, comparing normalized upper is likely enough for now.
                            # Also checked at source level, but double check record integrity
                            if record_state != target_state:
                                drop_counts["filtered_state"] += 1
                                continue

                        # 3. Check Status
                        if unified.get("status") != "Active":
                            drop_counts["inactive"] += 1
                            continue
                        
                        # 4. Check Duplicate (Content-based fingerprint)
                        # Use a tuple for the key
                        fingerprint = (unified.get("name"), full_address)
                        
                        if fingerprint in seen_entries:
                            drop_counts["duplicate"] += 1
                            continue
                        else:
                            seen_entries.add(fingerprint)
                            
                        outfile.write(json.dumps(unified) + "\n")
                        count += 1
                        if count % 10 == 0:
                            print(f"  Processed {count} records...", end="\r")
                    except Exception as e:
                        print(f"\n  Error processing record: {e}")
                        
                print(f"\nCompleted {source['name']}: {count} records written.")
                
            except FileNotFoundError:
                print(f"Error: File {source['path']} not found.")
    
    print("Unification complete.")
    print("\nDropped Records Summary:")
    for reason, count in drop_counts.items():
        print(f"  {reason}: {count}")


if __name__ == "__main__":
    main()
