#!/usr/bin/env python3
"""
Validation script for tuple unpacking consistency in occurrence_reports.py handler.

This script detects when tuple structures are changed (new child models added) but not
all unpacking locations are updated, which would cause ValueError: too many values to unpack.

Usage:
    python validate_tuple_unpacking.py

Exit codes:
    0 = All checks passed
    1 = Validation failed
"""

import re
import sys
from pathlib import Path


def extract_tuple_size(unpack_line_group: str) -> int:
    """
    Extract the number of elements in a tuple unpacking.

    Examples:
        "inst, habitat_data, habitat_condition, submitter_information_data, location_data = up"
        Returns 5
    """
    # Find the assignment part (before = or in parentheses)
    if "=" in unpack_line_group:
        left_side = unpack_line_group.split("=")[0]
    else:
        left_side = unpack_line_group

    # Remove "for" and parentheses, handle multi-line
    vars_str = left_side.replace("for", "").replace("(", "").replace(")", "").strip()

    # Split by comma, but be careful with strings and nested structures
    # For now, simple split should work for our tuple unpacking
    vars_list = [v.strip() for v in vars_str.split(",") if v.strip()]

    # Remove 'in' if it got included (shouldn't, but just in case)
    vars_list = [v for v in vars_list if not v.startswith("in")]

    return len(vars_list)


def find_unpacking_locations(handler_path: str) -> dict:
    """
    Find all tuple unpacking locations in the handler file by looking for
    multiline tuple unpacking patterns and counting commas.

    Returns:
        {
            "to_update_1": {"line": 1264, "size": 5, "context": "..."},
            "to_update_2": {"line": 1605, "size": 5, "context": "..."},
            "create_meta": {"line": 1726, "size": 5, "context": "..."},
        }
    """
    with open(handler_path) as f:
        lines = f.readlines()

    locations = {}
    i = 0
    to_update_count = 0

    while i < len(lines):
        line = lines[i]

        # Pattern 1: "for up in to_update:" followed by multiline tuple unpacking
        if " in to_update:" in line:
            to_update_count += 1

            # Collect tuple content between parens
            # The pattern is:
            #     for up in to_update:
            #         (
            #             var1,
            #             var2,
            #             ...
            #         ) = up

            j = i + 1
            # Look for opening paren
            while j < len(lines) and "(" not in lines[j]:
                j += 1

            if j < len(lines):
                # Count lines until closing paren with assignment
                tuple_content = ""
                while j < len(lines):
                    tuple_content += lines[j]
                    if ") = " in lines[j] or ") in " in lines[j]:
                        break
                    j += 1

                # Count non-empty, non-paren lines
                size = 0
                for line_in_tuple in tuple_content.split("\n"):
                    stripped = line_in_tuple.strip()
                    # Count commas and remove them
                    if stripped and stripped not in ("(", ")", ") = up"):
                        # Each line either has a var, or var with comma
                        if stripped.endswith(","):
                            size += 1
                        elif stripped and not stripped.startswith(")"):
                            size += 1

                location_key = f"to_update_{to_update_count}"
                locations[location_key] = {
                    "line": i + 1,
                    "size": size,
                    "context": tuple_content.replace("\n", " ").strip()[:100],
                }

            i += 1

        # Pattern 2: "for (" with multiline unpacking and " in create_meta:"
        elif "for (" in line and " in " not in line:
            # Multi-line tuple unpacking, need to find " in create_meta:"
            unpacking = line
            j = i + 1
            while j < len(lines):
                unpacking += lines[j]
                if " in create_meta:" in lines[j]:
                    break
                j += 1

            # Count variables between parens and " in"
            before_in = unpacking.split(" in create_meta:")[0]
            # Remove 'for' and find content between parens
            after_for = before_in.split("for ")[1] if "for " in before_in else before_in
            # Find content between ( and )
            if "(" in after_for and ")" in after_for:
                after_open = after_for.split("(")[1]
                before_close = after_open.split(")")[0]
                # Split by comma and count non-empty parts
                vars_list = [v.strip() for v in before_close.split(",") if v.strip()]
                size = len(vars_list)
            else:
                size = 0

            locations["create_meta"] = {
                "line": i + 1,
                "size": size,
                "context": unpacking.replace("\n", " ").strip()[:100],
            }
            i = j + 1
        else:
            i += 1

    return locations


def validate_tuple_consistency(handler_path: str) -> tuple[bool, str]:
    """
    Validate that all unpacking locations use the same tuple size.

    Returns:
        (is_valid, message)
    """
    locations = find_unpacking_locations(handler_path)

    if not locations:
        return False, "ERROR: No tuple unpacking locations found!"

    # Get all sizes
    sizes = {loc: info["size"] for loc, info in locations.items()}
    unique_sizes = set(sizes.values())

    if len(unique_sizes) == 1:
        size = unique_sizes.pop()
        msg = f"✓ PASS: All {len(locations)} unpacking locations use {size} elements\n"
        for loc, info in sorted(locations.items()):
            msg += "  - " + loc + f" (line {info['line']}): {info['size']} elements\n"
        return True, msg
    else:
        msg = "✗ FAIL: Tuple sizes are inconsistent!\n"
        msg += f"Found {len(unique_sizes)} different sizes: {sorted(unique_sizes)}\n\n"
        for loc, info in sorted(locations.items()):
            msg += f"  - {loc} (line {info['line']}): {info['size']} elements\n"
            msg += f"    {info['context']}\n"
        msg += "\nLikely cause: Added a new child model but didn't update all unpacking locations.\n"
        msg += "See TUPLE_UNPACKING_VALIDATION.md for the complete list of locations.\n"
        return False, msg


def validate_append_consistency(handler_path: str) -> tuple[bool, str]:
    """
    Validate that tuple appending matches unpacking expectations.

    Returns:
        (is_valid, message)
    """
    locations = find_unpacking_locations(handler_path)

    if not locations:
        return False, "ERROR: No tuple unpacking locations found!"

    # Expected sizes by append location
    with open(handler_path) as f:
        content = f.read()

    # Find to_update.append and create_meta.append calls
    to_update_appends = re.findall(
        r"to_update\.append\s*\(\s*\((.*?)\)\s*\)", content, re.DOTALL
    )
    create_meta_appends = re.findall(
        r"create_meta\.append\s*\(\s*\((.*?)\)\s*\)", content, re.DOTALL
    )

    issues = []

    # Check to_update.append sizes
    for append_content in to_update_appends:
        items = [x.strip() for x in append_content.split(",") if x.strip()]
        append_size = len(items)

        # Compare with unpacking sizes
        to_update_sizes = [
            info["size"]
            for loc, info in locations.items()
            if loc.startswith("to_update")
        ]

        if to_update_sizes and append_size not in to_update_sizes:
            issues.append(
                f"to_update.append has {append_size} elements but unpacking expects "
                f"{to_update_sizes}"
            )

    # Check create_meta.append sizes
    for append_content in create_meta_appends:
        items = [x.strip() for x in append_content.split(",") if x.strip()]
        append_size = len(items)

        # Compare with unpacking sizes
        if "create_meta" in locations:
            unpack_size = locations["create_meta"]["size"]
            if append_size != unpack_size:
                issues.append(
                    f"create_meta.append has {append_size} elements but "
                    f"unpacking at line {locations['create_meta']['line']} expects {unpack_size}"
                )

    if issues:
        msg = "✗ FAIL: Append/unpacking mismatch detected!\n"
        for issue in issues:
            msg += f"  - {issue}\n"
        return False, msg
    else:
        msg = "✓ PASS: All append statements match their corresponding unpacking\n"
        return True, msg


def main():
    handler_path = Path(__file__).parent / "handlers" / "occurrence_reports.py"

    if not handler_path.exists():
        print(f"ERROR: Handler file not found: {handler_path}")
        return 1

    print(f"Validating tuple unpacking in {handler_path.name}...\n")

    # Check 1: Tuple consistency
    valid_1, msg_1 = validate_tuple_consistency(str(handler_path))
    print(msg_1)

    # Check 2: Append consistency
    valid_2, msg_2 = validate_append_consistency(str(handler_path))
    print(msg_2)

    if valid_1 and valid_2:
        print("\n✓ All validations passed!")
        return 0
    else:
        print("\n✗ Some validations failed. See TUPLE_UNPACKING_VALIDATION.md")
        return 1


if __name__ == "__main__":
    sys.exit(main())
