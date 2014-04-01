if __name__ == "__main__":
    import sys
    fname = sys.argv[1]
    with open(fname, 'rU') as f:
        sys.stdout.write(f.read())
