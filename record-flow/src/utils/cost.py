"""Cost tracking and reporting utilities."""
from config import PRICING


def print_cost_summary(cost_snapshot: dict):
    """Print final cost summary from cost tracker snapshot."""
    print("\n=== Token Usage & Cost Estimate ===")
    total_cost = 0.0

    step_pricing_map = {
        "gemini_search": "gemini",
        "gemini_finalizer": "gemini"
    }

    for step, tokens in cost_snapshot.items():
        input_tokens = tokens["input"]
        output_tokens = tokens["output"]

        pricing_key = step_pricing_map.get(step)
        if pricing_key and pricing_key in PRICING:
            rates = PRICING[pricing_key]
            input_cost = (input_tokens / 1_000_000) * rates["input"]
            output_cost = (output_tokens / 1_000_000) * rates["output"]
            step_cost = input_cost + output_cost
            total_cost += step_cost

            print(f"Step: {step}")
            print(f"  Input Tokens:  {input_tokens:,}")
            print(f"  Output Tokens: {output_tokens:,}")
            print(f"  Estimated Cost: ${step_cost:.4f}")
        else:
            print(f"Step: {step} (No pricing data)")
            print(f"  Input Tokens:  {input_tokens:,}")
            print(f"  Output Tokens: {output_tokens:,}")

    print(f"-----------------------------------")
    print(f"Total Estimated Cost: ${total_cost:.4f}")
    print(f"===================================")
