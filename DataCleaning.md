### Data Cleansing Operations

# Customers table 
# Transactions table

1. The need to remove padding - done
2. Check for blanks done
3. Check for nulls -done
4. Check for N/A - done
5. Change to upper

For keys specifically, the mechanical sequence:

Map junk to null — empty string, whitespace-only, and literal "NULL", "NA", "N/A", "-", "None".
Trim leading and trailing whitespace.
Case-fold to upper (or lower — just pick one and use it everywhere).
Strip cosmetic punctuation, if the key has any that doesn't carry meaning — hyphens, spaces inside the value.
Settle leading zeros — pad to a fixed width, or strip them. Padding is safer.
Filter out rows where the key is now null.
Dedupe, last.
Verify — row count should equal distinct key count.

