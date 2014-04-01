def make_floats(reader, cols):
    rows = []
    for row in reader:
        for col in cols:
            idx = int(col)
            try:
                row[idx] = float(row[idx])
            except ValueError:
                row[idx] = None
        rows.append(row)
    return rows

if __name__ == "__main__":
    # Casts values in given indexes as floats for all rows.
    import sys
    import csv
    cols = sys.argv[1].split(',')
    reader = csv.reader(sys.stdin)
    rows = make_floats(reader, cols)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
