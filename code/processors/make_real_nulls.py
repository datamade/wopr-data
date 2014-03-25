if __name__ == "__main__":
    # Turns things like "NA" and "N/A" into a NoneType
    import sys
    import csv
    reader = csv.reader(sys.stdin)
    rows = []
    for row in reader:
        for idx, val in enumerate(row):
            if val.strip() in ['NA', 'N/A', '-']:
                row[idx] = None
        rows.append(row)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
