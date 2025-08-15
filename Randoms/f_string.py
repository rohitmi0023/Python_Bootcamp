# DETAILED EXPLANATION OF STRING FORMATTING
# ==========================================

# Original code:
# print(f"{stat['Column']:<15} | {stat['Data Type']:<12} | "
#       f"{stat['Memory (MB)']:>8.2f} MB | {stat['Unique Values']:>8,} unique")

# Let's break this down step by step with examples

print("UNDERSTANDING F-STRING FORMATTING SYNTAX")
print("=" * 50)

# Sample data to demonstrate with
sample_stat = {
    'Column': 'customer_id',
    'Data Type': 'int64', 
    'Memory (MB)': 7.629394531,
    'Unique Values': 125000
}

print("\n1. BASIC F-STRING SYNTAX")
print("-" * 30)
print("f-strings use curly braces {} to embed expressions inside strings")
print(f"Example: f\"Hello {sample_stat['Column']}\"")
print(f"Result: Hello {sample_stat['Column']}")

print("\n2. ALIGNMENT SPECIFIERS")
print("-" * 30)
print("Format: {value:alignment_character width}")
print("< = left align, > = right align, ^ = center align")

# Left alignment examples
column_name = sample_stat['Column']
print(f"\nLeft alignment examples:")
print(f"'{column_name:<10}'  # <10 means left-align in 10 characters")
print(f"Result: '{column_name:<10}'")
print(f"'{column_name:<15}'  # <15 means left-align in 15 characters") 
print(f"Result: '{column_name:<15}'")

# Right alignment examples  
memory_value = sample_stat['Memory (MB)']
print(f"\nRight alignment examples:")
print(f"'{memory_value:>10}'   # >10 means right-align in 10 characters")
print(f"Result: '{memory_value:>10}'")
print(f"'{memory_value:>15}'   # >15 means right-align in 15 characters")
print(f"Result: '{memory_value:>15}'")

print("\n3. PRECISION SPECIFIERS FOR NUMBERS")
print("-" * 40)
print("Format: {value:alignment width.precision f}")
print(".2f means 2 decimal places for floats")

memory_precise = sample_stat['Memory (MB)']
print(f"\nPrecision examples:")
print(f"Original value: {memory_precise}")
print(f"With .2f: {memory_precise:.2f}")
print(f"With .1f: {memory_precise:.1f}")
print(f"With .4f: {memory_precise:.4f}")

print(f"\nCombined alignment + precision:")
print(f"'>8.2f' means right-align in 8 chars with 2 decimals")
print(f"'{memory_precise:>8.2f}'")

print("\n4. THOUSAND SEPARATORS")
print("-" * 30)
print("The comma (,) adds thousand separators to numbers")

unique_count = sample_stat['Unique Values']
print(f"\nThousand separator examples:")
print(f"Original: {unique_count}")
print(f"With comma: {unique_count:,}")
print(f"Right-aligned with comma: {unique_count:>10,}")

print("\n5. PUTTING IT ALL TOGETHER")
print("-" * 35)
print("Let's recreate the original format string step by step:")

# Breaking down each part of the original format string
col = sample_stat['Column']
dtype = sample_stat['Data Type'] 
memory = sample_stat['Memory (MB)']
unique = sample_stat['Unique Values']

print(f"\nOriginal components:")
print(f"Column: '{col}'")
print(f"Data Type: '{dtype}'")
print(f"Memory: {memory}")
print(f"Unique Values: {unique}")

print(f"\nStep-by-step formatting:")
print(f"1. '{col:<15}' = '{col:<15}'")
print(f"2. '{dtype:<12}' = '{dtype:<12}'") 
print(f"3. '{memory:>8.2f}' = '{memory:>8.2f}'")
print(f"4. '{unique:>8,}' = '{unique:>8,}'")

print(f"\nFinal formatted string:")
formatted_line = f"{col:<15} | {dtype:<12} | {memory:>8.2f} MB | {unique:>8,} unique"
print(f"'{formatted_line}'")

print("\n6. VISUAL BREAKDOWN OF ALIGNMENT")
print("-" * 40)

# Show the character positions
print("Position markers (every 5 characters):")
print("12345678901234567890123456789012345678901234567890123456789012345678901234567890")
print(formatted_line)

# Show what each section takes up
print("\nSection breakdown:")
print("←-- 15 chars --→")
print(f"{col:<15}", end="")
print(" |")
print("                ←-- 12 chars --→") 
print("                ", end="")
print(f"{dtype:<12}", end="")
print(" |")
print("                                 ←8→")
print("                                 ", end="")
print(f"{memory:>8.2f}", end="")
print(" MB |")
print("                                            ←--8--→")
print("                                            ", end="")
print(f"{unique:>8,}", end="")
print(" unique")

print("\n7. WHY USE THIS FORMATTING?")
print("-" * 35)
print("""
This formatting creates clean, readable tables because:

• CONSISTENT COLUMNS: Each piece of data takes up exactly the allocated space
• ALIGNED DATA: Numbers align on the right (easier to compare), text on left
• READABLE: The pipe separators (|) create clear visual columns
• PROFESSIONAL: Looks like a proper data table, not messy scattered text

Compare these outputs:

UNFORMATTED (messy):
customer_id | int64 | 7.629394531 MB | 125000 unique
age | int8 | 0.95 MB | 95 unique

FORMATTED (clean):
customer_id     | int64        |     7.63 MB |  125,000 unique
age             | int8         |     0.95 MB |       95 unique
""")

print("\n8. COMMON FORMAT SPECIFIERS CHEAT SHEET")
print("-" * 45)
format_examples = [
    ("{value:<10}", "Left align in 10 characters"),
    ("{value:>10}", "Right align in 10 characters"), 
    ("{value:^10}", "Center align in 10 characters"),
    ("{value:.2f}", "2 decimal places"),
    ("{value:,.0f}", "No decimals with thousand separators"),
    ("{value:>10.2f}", "Right align, 10 chars, 2 decimals"),
    ("{value:0>5}", "Right align, pad with zeros to 5 chars"),
    ("{value:_<10}", "Left align, pad with underscores to 10 chars")
]

for format_spec, description in format_examples:
    print(f"{format_spec:<15} → {description}")

print("\n9. PRACTICAL EXAMPLE WITH DIFFERENT DATA")
print("-" * 45)

# Show how it works with various data types and sizes
sample_data = [
    {'Column': 'id', 'Data Type': 'int8', 'Memory (MB)': 0.95, 'Unique Values': 50},
    {'Column': 'customer_name', 'Data Type': 'object', 'Memory (MB)': 125.7, 'Unique Values': 50000},
    {'Column': 'amount', 'Data Type': 'float32', 'Memory (MB)': 3.8, 'Unique Values': 15000},
    {'Column': 'category', 'Data Type': 'category', 'Memory (MB)': 0.01, 'Unique Values': 5}
]

print("\nExample table output:")
print("-" * 65)
for stat in sample_data:
    print(f"{stat['Column']:<15} | {stat['Data Type']:<12} | "
          f"{stat['Memory (MB)']:>8.2f} MB | {stat['Unique Values']:>8,} unique")

print("\nNotice how everything lines up perfectly in columns!")