#!/usr/bin/env python3
"""Example usage of the new case() function in Daft.

This demonstrates both simple case and searched case expressions,
showing the improved programmatic construction capabilities.
"""
from __future__ import annotations

import daft
from daft.functions import case, when


def main():
    print("=== Daft Case Function Examples ===\n")

    # Example 1: Simple Case (Value Matching)
    print("1. Simple Case - Grade to GPA conversion:")
    df_grades = daft.from_pydict({"grade": ["A", "B", "C", "D", "F", "A+"]})

    gpa_result = df_grades.select(
        df_grades["grade"],
        case(
            df_grades["grade"],
            [
                ("A+", 4.3),
                ("A", 4.0),
                ("B", 3.0),
                ("C", 2.0),
                ("D", 1.0),
            ],
            else_=0.0,
        ).alias("gpa"),
    )
    print(gpa_result.collect())
    print()

    # Example 2: Searched Case (Boolean Conditions)
    print("2. Searched Case - Score to Grade conversion:")
    df_scores = daft.from_pydict(
        {"student": ["Alice", "Bob", "Charlie", "Diana", "Eve"], "score": [95, 87, 78, 65, 92]}
    )

    grade_result = df_scores.select(
        df_scores["student"],
        df_scores["score"],
        case(
            branches=[
                (df_scores["score"] >= 90, "A"),
                (df_scores["score"] >= 80, "B"),
                (df_scores["score"] >= 70, "C"),
                (df_scores["score"] >= 60, "D"),
            ],
            else_="F",
        ).alias("grade"),
    )
    print(grade_result.collect())
    print()

    # Example 3: Programmatic Construction
    print("3. Programmatic Construction - Dynamic grade mapping:")

    # Build grade mapping programmatically
    grade_thresholds = [
        (90, "Excellent"),
        (80, "Good"),
        (70, "Satisfactory"),
        (60, "Needs Improvement"),
    ]

    # Create branches dynamically
    branches = [(df_scores["score"] >= threshold, grade) for threshold, grade in grade_thresholds]

    performance_result = df_scores.select(
        df_scores["student"], df_scores["score"], case(branches=branches, else_="Unsatisfactory").alias("performance")
    )
    print(performance_result.collect())
    print()

    # Example 4: Complex Conditions
    print("4. Complex Conditions - Age and income categorization:")
    df_people = daft.from_pydict(
        {
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 17, 35, 70, 45],
            "income": [50000, 0, 75000, 30000, 90000],
        }
    )

    category_result = df_people.select(
        df_people["name"],
        df_people["age"],
        df_people["income"],
        case(
            branches=[
                ((df_people["age"] >= 18) & (df_people["age"] < 65) & (df_people["income"] > 40000), "Working Adult"),
                (
                    (df_people["age"] >= 18) & (df_people["age"] < 65) & (df_people["income"] <= 40000),
                    "Low Income Adult",
                ),
                (df_people["age"] < 18, "Minor"),
                (df_people["age"] >= 65, "Senior"),
            ],
            else_="Other",
        ).alias("category"),
    )
    print(category_result.collect())
    print()

    # Example 5: Comparison with traditional when() chaining
    print("5. Comparison - case() vs when() chaining:")

    print("Using case():")
    case_result = df_scores.select(
        case(
            branches=[
                (df_scores["score"] >= 90, "A"),
                (df_scores["score"] >= 80, "B"),
                (df_scores["score"] >= 70, "C"),
            ],
            else_="F",
        ).alias("grade_case")
    )
    print(case_result.collect())
    print()

    print("Using when() chaining:")
    when_result = df_scores.select(
        when(df_scores["score"] >= 90, then="A")
        .when(df_scores["score"] >= 80, then="B")
        .when(df_scores["score"] >= 70, then="C")
        .otherwise("F")
        .alias("grade_when")
    )
    print(when_result.collect())
    print()

    # Example 6: Null handling
    print("6. Null Handling:")
    df_with_nulls = daft.from_pydict({"value": [10, None, 25, 0, None, 15]})

    null_result = df_with_nulls.select(
        df_with_nulls["value"],
        case(
            branches=[
                (df_with_nulls["value"].is_null(), "Missing"),
                (df_with_nulls["value"] == 0, "Zero"),
                (df_with_nulls["value"] > 20, "High"),
                (df_with_nulls["value"] > 0, "Positive"),
            ],
            else_="Other",
        ).alias("status"),
    )
    print(null_result.collect())
    print()


if __name__ == "__main__":
    main()
