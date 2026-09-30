import os
import matplotlib.pyplot as plt
import numpy as np

os.makedirs(r"e:\Github\big-data-mini-project\docs\figures", exist_ok=True)

# Set global styles
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['axes.labelweight'] = 'bold'
plt.rcParams['figure.titlesize'] = 14

# --- Figure 1: Fleet Utilization by Zone ---
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5), dpi=300)

zones = ['Downtown', 'Airport', 'Uptown', 'Suburbs']
active = [7, 5, 4, 2]
enroute = [2, 1, 2, 1]
idle = [1, 0, 2, 3]
idle_ratio = [10.0, 0.0, 25.0, 50.0]
earnings = [412.50, 385.00, 210.00, 95.50]

x = np.arange(len(zones))
width = 0.22

rects1 = ax1.bar(x - width, active, width, label='Active (On Trip)', color='#1f77b4')
rects2 = ax1.bar(x, enroute, width, label='Enroute', color='#2ca02c')
rects3 = ax1.bar(x + width, idle, width, label='Idle', color='#d62728')

ax1.set_ylabel('Vehicle Count')
ax1.set_title('(a) Real-Time Vehicle Status by Grid Zone')
ax1.set_xticks(x)
ax1.set_xticklabels(zones)
ax1.legend(loc='upper right', frameon=True)
ax1.grid(axis='y', linestyle='--', alpha=0.5)

# Zone Earnings & Idle Ratio on secondary axis
ax2_twin = ax2.twinx()
bars = ax2.bar(x, earnings, width=0.45, color='#4575b4', alpha=0.85, label='Window Earnings ($)')
line = ax2_twin.plot(x, idle_ratio, color='#d73027', marker='o', linewidth=2.5, markersize=8, label='Idle Ratio (%)')

ax2.set_ylabel('Real-Time Earnings ($)', color='#4575b4')
ax2_twin.set_ylabel('Idle Ratio (%)', color='#d73027')
ax2.set_title('(b) Zone Earnings vs. Idle Ratio')
ax2.set_xticks(x)
ax2.set_xticklabels(zones)
ax2.grid(axis='y', linestyle='--', alpha=0.5)
ax2_twin.set_ylim(-5, 65)

# Combine legends
lines_1, labels_1 = ax2.get_legend_handles_labels()
lines_2, labels_2 = ax2_twin.get_legend_handles_labels()
ax2.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper right', frameon=True)

plt.tight_layout()
fig.savefig(r"e:\Github\big-data-mini-project\docs\figures\fleet_utilization_chart.png", bbox_inches='tight')
plt.close(fig)

# --- Figure 2: Daily Profitability Reconciliation ---
fig, ax = plt.subplots(figsize=(10, 4.8), dpi=300)

vehicles = ['VEH-101', 'VEH-103', 'VEH-105', 'VEH-108', 'VEH-112', 'VEH-114', 'VEH-118', 'VEH-121']
gross = [185.0, 210.5, 160.0, 112.0, 195.0, 135.0, 220.0, 68.0]
fuel = [38.5, 48.2, 42.0, 52.8, 45.0, 118.4, 51.0, 41.5]
maint = [15.0, 15.0, 20.0, 145.0, 25.0, 18.0, 12.0, 35.0]
net = [round(g - (f + m), 2) for g, f, m in zip(gross, fuel, maint)]

x = np.arange(len(vehicles))
width = 0.35

ax.bar(x - width/2, gross, width, label='Gross Fare Revenue', color='#2b83ba', alpha=0.9)
exp_bars = ax.bar(x + width/2, [f + m for f, m in zip(fuel, maint)], width, label='Total Expenses (Fuel + Maint)', color='#fdae61', alpha=0.9)

# Overlay net profit indicator line
ax2 = ax.twinx()
net_colors = ['#1a9641' if n >= 0 else '#d7191c' for n in net]
ax2.scatter(x, net, color=net_colors, s=80, zorder=5)
for i, n in enumerate(net):
    offset = 8 if n >= 0 else -14
    ax2.annotate(f"${n:+.1f}", (x[i], n + offset), ha='center', fontsize=8.5, fontweight='bold',
                 color='#1a9641' if n >= 0 else '#d7191c')

ax2.plot(x, net, linestyle=':', color='gray', alpha=0.6, zorder=4)
ax2.axhline(0, color='red', linestyle='--', linewidth=1, alpha=0.7)
ax2.set_ylabel('Net Profit ($)', color='#1a9641', fontweight='bold')
ax2.set_ylim(-120, 180)

ax.set_ylabel('Amount ($)')
ax.set_title('Reconciled Vehicle Financials: Gross Revenue vs. Total Operational Expenses')
ax.set_xticks(x)
ax.set_xticklabels(vehicles)
ax.legend(loc='upper left', frameon=True)
ax.grid(axis='y', linestyle='--', alpha=0.5)

plt.tight_layout()
fig.savefig(r"e:\Github\big-data-mini-project\docs\figures\profitability_reconciliation.png", bbox_inches='tight')
plt.close(fig)

print("Generated figures successfully!")
