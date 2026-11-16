"""show_model_results.py - print the model results copied into results/ by stage2_status.sh."""
import csv, glob

def read(name):
    f = glob.glob(f"results/{name}/*.csv")
    return list(csv.DictReader(open(f[0]))) if f else []
num = lambda v, d=3: "-" if v in (None, "", "null") else f"{float(v):.{d}f}"

print("== metrics (higher AUC is better; lower log loss and Brier are better)")
print(f"{'model':<11} {'split':<6} {'AUC':>6} {'PR-AUC':>7} {'logloss':>8} {'Brier':>6} {'late':>6} {'mean_p':>7} {'rows':>11}")
for r in sorted(read("model_metrics"), key=lambda r: (["valid", "test", "covid"].index(r["split"]), r["model"])):
    print(f"{r['model']:<11} {r['split']:<6} {num(r['auc_roc']):>6} {num(r['auc_pr']):>7} {num(r['logloss']):>8} "
          f"{num(r['brier']):>6} {num(r['late_rate']):>6} {num(r['mean_pred']):>7} {int(float(r['n'])):>11,}")

print("\n== calibration, full model, test 2025 (predicted vs actual late rate by risk decile)")
for r in sorted((r for r in read("calibration") if r["model"] == "full" and r["split"] == "test"),
                key=lambda r: int(r["decile"])):
    d = int(r["decile"])
    print(f"  risk {d*10:>2}-{d*10+10:<3}%  flights {int(float(r['n'])):>9,}  predicted {num(r['mean_pred'])}  actual {num(r['actual'])}")

print("\n== segments, test 2025")
for r in read("segments"):
    if r["split"] == "test":
        print(f"  {r['segment']:<15} {r['model']:<11} AUC {num(r['auc_roc'])}  late {num(r['late_rate'])}  "
              f"predicted {num(r['mean_pred'])}  n {int(float(r['n'])):,}")

print("\n== strongest inputs, full model (standardized coefficient; + raises risk)")
for r in sorted(read("coefficients"), key=lambda r: -float(r["abs_coef"]))[:20]:
    print(f"  {r['feature']:<18} {float(r['coef_standardized']):+.3f}")
