from matplotlib import pyplot as plt
import os, os.path
import pickle
import re

def draw_rocs():

    plt.figure(figsize=(10,10))
    plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC curve')

    targetc = re.compile(r"^(.+).fprtpr$")
    for (dpath, dnames, fnames) in os.walk('rocs/'):
        fnames.sort()
        for fn in fnames:
            fn_exc_suf = targetc.match(fn)
            if fn_exc_suf:
                with open(dpath+fn, 'rb') as fin:
                    fprtpr = pickle.load(fin)

                    fpr, tpr, thresholds = fprtpr['fpr'], fprtpr['tpr'], fprtpr['thresholds']
                    maxI, maxD, stepS, roc_auc = fprtpr['maxI'], fprtpr['maxD'], fprtpr['stepS'], fprtpr['roc_auc']

                    #draw roc curve
                    plt.plot(fpr, tpr, lw=2,
                        label='maxI{}maxD{}stepS{}(area = {:.2f})' \
                        .format(maxI, maxD, stepS, roc_auc))

        break # just one depth


    plt.legend(loc="lower right")
    plt.savefig("roc.jpg")
    plt.show()


if __name__ == '__main__':
    draw_rocs()

