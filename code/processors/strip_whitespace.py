if __name__ == "__main__":
    import sys
    import csv
    reader = csv.reader(sys.stdin)
    rows = []
    for row in reader:
        for idx,val in enumerate(row):
            row[idx] = val.strip()
        rows.append(row)
    writer = csv.writer(sys.stdout)
    writer.writerows(rows)
