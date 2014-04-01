def nullify_fields(reader, chars):
    rows = []
    for row in reader:
        for i, val in enumerate(row):
            if val in chars:
                row[i] = None
        rows.append(row)
    return rows

if __name__ == "__main__":
    # Finds fields with certain values and nulls them
    import sys
    import csv
    chars = sys.argv[1].split(',')
    reader = csv.reader(sys.stdin)
    rows = nullify_fields(reader, chars)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)

