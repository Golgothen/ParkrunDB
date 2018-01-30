import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--country', nargs = '+', default = 'Australia',  help = 'Specify a country to import by name. Surround the country name with double quotes if it contains a space.')
args = parser.parse_args()
print(args)
for c in args.country:
    print(c)