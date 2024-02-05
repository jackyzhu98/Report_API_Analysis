import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

font = fm.FontProperties(fname = 'c:\\windows\\fonts\\simsun.ttc')

categories = ['銷售量', '銷售收入','產品價格','産品評分', '評論量', '排名']

# corresponding ids: rating, revenue, sales, ranking, review, npl, pd, score
print(categories)

categories = [*categories, categories[0]]
print(categories)
# shop1 = [52.3, 12.66, 12.5, 57.85, 20.1, 14.99, 36.31, 15.15]
# shop1 = [100 - x for x in shop1]
# shop1 = [*shop1, shop1[0]]
sid = "ARNP8FTZEG83T"

whole = [1,0.8333,0.3333,0,0.5,0.3333]

k = 60

whole = [k*(x) + 40 for x in whole]

whole = [*whole, whole[0]]
print(whole)
# shop2_group = [0.324404762, 0.095238095, 0.168154762, 0.263392857, 0.120535714, 0.4539, 0.2738, 0.0056]
# shop2_group = [100 - (100 * x) for x in shop2_group]
# shop2_group = [*shop2_group, shop2_group[0]]

ref1 = [100] * len(categories)

label_loc = np.linspace(start = 0, stop = 2 * np.pi, num = len(categories))
label_loc += label_loc[:1]


fig, ax = plt.subplots(figsize = (5,5), subplot_kw = dict(polar = True), dpi = 300)

ax.plot(label_loc, ref1, color = 'gray', linewidth = 1)
ax.fill(label_loc, ref1, color = 'gray', alpha = 0.05)

ax.plot(label_loc, whole, color = 'green', linewidth = 1.5)
ax.fill(label_loc, whole, color = 'green', alpha = 0.25)

ax.set_theta_offset(np.pi / 2)
ax.set_theta_direction(-1)

ax.set_thetagrids(np.degrees(label_loc), labels = categories, fontproperties = font)


for label, angle in zip(ax.get_xticklabels(), label_loc):
    if angle in (0, np.pi, 2 * np.pi):
        label.set_horizontalalignment('center')
    elif 0 < angle < np.pi:
        label.set_horizontalalignment('left')
    else:
        label.set_horizontalalignment('right')
        

ax.set_ylim(0, 100)
ax.set_yticklabels([])
ax.set_rlabel_position(180 / len(categories))
ax.tick_params(axis = 'y', labelsize = 8)
ax.set_facecolor('#FAFAFA')


plt.savefig("D:\合作店铺\WHX\spider\/" + sid + '.jpg', dpi = 200)
