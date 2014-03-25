if __name__ == "__main__":
    # Combines columns at given indexes and adds them to 
    # the combined values (joined with "_") to the end of row
    import sys
    import csv
    cols = sys.argv[1].split(',')
    reader = csv.reader(sys.stdin)
    rows = []
    for row in reader:
        vals = []
        for col in cols:
            vals.append(row[int(col)])
        row.append('-'.join(vals))
        rows.append(row)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
