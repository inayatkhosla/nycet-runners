import sys
import lib.comp_metrics_calculator as cmc

def main():
    script = sys.argv[0]
    password = (sys.argv[1])
    for i in ('ed','hl'):
        mg = cmc.MetricsGenerator(i, password)
        mg.run()

main()
