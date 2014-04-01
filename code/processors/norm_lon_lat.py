def norm_lon_lat(reader, cols):
    header = reader.next()
    header = [h.lower() for h in header]
    rows = [header]
    for row in reader:
        for col in cols:
            idx = int(col)
            cleaned = row[idx].replace('+','')
            row[idx] = float(cleaned) / 1000
        rows.append(row)
    return rows

if __name__ == "__main__":
    # Throws out rows that contain null values in given indexes
    import sys
    import csv
    cols = sys.argv[1].split(',')
    reader = csv.reader(sys.stdin)
    rows = norm_lon_lat(reader, cols)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)

