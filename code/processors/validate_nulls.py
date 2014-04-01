def validate_nulls(reader, cols):
    rows = []
    for row in reader:
        vals = True
        for col in cols:
            idx = int(col)
            if row[idx].strip() == '':
                vals = False
        if vals:
            rows.append(row)
    return rows

if __name__ == "__main__":
    # Throws out rows that contain null values in given indexes
    import sys
    import csv
    cols = sys.argv[1].split(',')
    reader = csv.reader(sys.stdin)
    rows = validate_nulls(reader, cols)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
