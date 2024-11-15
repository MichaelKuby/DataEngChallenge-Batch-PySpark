from fractions import Fraction
import re


def parse_measurement(measurement_str):
    # Remove any parentheses or additional text
    measurement_str = re.sub(r"\(.*?\)", "", measurement_str).strip()

    # Parse mixed numbers and fractions
    try:
        if measurement_str:
            total = 0.0
            # Split by spaces to handle mixed numbers
            parts = measurement_str.split()
            for part in parts:
                if "/" in part:
                    # It's a fraction
                    total += float(Fraction(part))
                else:
                    # It's a whole number
                    total += float(part)
            return total
        else:
            return None
    except Exception:
        return None


def parse_pattern_a(dim_str):
    result = {
        "Height": None,
        "Width": None,
        "Length": None,
        "Diameter": None,
        "Unit": None,
    }

    # Regex to match label, measurement, unit
    match = re.match(r"^(H\.|Diam\.)\s*(.*?)\s*(in\.|cm)", dim_str)

    if match:
        label = match.group(1)
        measurement = match.group(2)
        unit = match.group(3)

        value = parse_measurement(measurement)

        if label == "H.":
            result["Height"] = value
        elif label == "Diam.":
            result["Diameter"] = value

        result["Unit"] = unit

    return result


def parse_pattern_b(dim_str):
    result = {
        "Height": None,
        "Width": None,
        "Length": None,
        "Diameter": None,
        "Unit": None,
    }

    # Check if 'Overall:' is present
    overall = False
    if dim_str.startswith("Overall:"):
        overall = True

    # Extract dimensions before units
    dimension_part = re.split(r"\s*(in\.|cm)\s*", dim_str)[0]
    unit_search = re.search(r"(in\.|cm)", dim_str)
    unit = unit_search.group(1) if unit_search else None

    # Remove labels like "Overall:"
    dimension_part = re.sub(r"^(Overall:)\s*", "", dimension_part)

    # Split dimensions by 'x', 'X', or '×'
    dimensions = re.split(r"\s*[xX×]\s*", dimension_part)

    # Parse each dimension
    dimension_values = [parse_measurement(d) for d in dimensions]

    # Map dimensions
    # Default order is Height x Width x Length
    dimension_names = ["Height", "Width", "Length"]

    # If specific pattern requires different mapping, adjust accordingly
    if overall:
        # If "Overall:" indicates a different order, adjust here if necessary
        pass  # In this case, we assume the default H x W x L order applies

    for i, value in enumerate(dimension_values):
        if i < len(dimension_names):
            result[dimension_names[i]] = value

    result["Unit"] = unit

    return result


def parse_pattern_c(dim_str):
    result = {
        "Height": None,
        "Width": None,
        "Length": None,
        "Diameter": None,
        "Unit": None,
    }

    # Split the string by semicolons
    parts = dim_str.split(";")

    for part in parts:
        part = part.strip()
        if part.startswith("H."):
            # Parse height
            match = re.match(r"^H\.\s*(.*?)\s*(in\.|cm)", part)
            if match:
                measurement = match.group(1)
                unit = match.group(2)
                value = parse_measurement(measurement)
                result["Height"] = value
                result["Unit"] = unit
        elif part.startswith("Diam."):
            # Parse diameter
            match = re.match(r"^Diam\.\s*(.*?)\s*(in\.|cm)", part)
            if match:
                measurement = match.group(1)
                unit = match.group(2)
                value = parse_measurement(measurement)
                result["Diameter"] = value
                result["Unit"] = unit

    return result


def parse_dimensions(dim_str):
    if dim_str is None:
        return {
            "Height": None,
            "Width": None,
            "Length": None,
            "Diameter": None,
            "Unit": None,
        }

    dim_str = dim_str.strip()

    # Determine pattern
    if re.match(r"^(H\.|Diam\.)", dim_str):
        if " x " in dim_str or " x" in dim_str or "x " in dim_str:
            # Pattern C: Combination of Labels and Dimensions
            return parse_pattern_c(dim_str)
        else:
            # Pattern A: Single Dimension with Label
            return parse_pattern_a(dim_str)
    elif (
        re.match(r"^(Overall:)", dim_str)
        or " x " in dim_str
        or " x" in dim_str
        or "x " in dim_str
    ):
        # Pattern B: Multiple Dimensions Separated by 'x'
        return parse_pattern_b(dim_str)
    else:
        # Default case or unrecognized pattern
        return {
            "Height": None,
            "Width": None,
            "Length": None,
            "Diameter": None,
            "Unit": None,
        }
