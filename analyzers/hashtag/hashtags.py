from itertools import accumulate, repeat
import random

# Let's pretend we parsed a dataset with these hashtags
hobby_hashtags = {
    "Photography": None,
    "Cooking": None,
    "Gardening": None,
    "Hiking": None,
    "Yoga": None,
    "Reading": None,
    "Painting": None,
    "Traveling": None,
    "Gaming": None,
    "Fitness": None,
    "Crafting": None,
    "Baking": None,
    "Cycling": None,
    "Music": None,
    "Dancing": None,
    "Writing": None,
    "Meditation": None,
    "Knitting": None,
    "Surfing": None,
    "Skating": None,
    "Running": None,
    "Climbing": None,
    "Fishing": None,
    "DIY": None,
    "Pottery": None,
    "Camping": None,
    "Blogging": None,
    "Coding": None,
    "Volunteering": None,
    "Astronomy": None,
    "Woodworking": None,
    "Calligraphy": None,
    "Sailing": None,
    "Skiing": None,
    "Scrapbooking": None,
    "Birdwatching": None,
    "Journaling": None,
    "Geocaching": None,
    "Cosplay": None,
    "Upcycling": None,
    "Podcasting": None,
    "Antiquing": None,
    "Beekeeping": None,
    "Homebrewing": None,
    "Archery": None,
    "Foraging": None,
    "Juggling": None,
    "Origami": None,
    "Stargazing": None,
    "Vlogging": None,
    "Mango": None,
    "Mangos": None,
    "MangoTrees": None,
    "LowHangingFruit": None,
    "Mangoes": None,
    "MangoTango": None,
    "MangoTime": None,
    "PickingMangos": None
}

# function for gini coefficient


def simulate_toy_cib(base_rate: int, injection_hashtags: dict, n_days: int, min_tweets:int, max_tweets: int, seed: int = 54321):

    # base behavior (no CIB)
    base = {key: base_rate for key in hobby_hashtags.keys()}
    for hasthtag, factor in injection_hashtags.items():
        base[hasthtag] = base_rate

    # now let's create a behavior where MangoTree hashtags are used more often (CIB)
    cib = {key: base_rate for key in hobby_hashtags.keys()}

    # just hand-pick some amplification factors
    for hasthtag, factor in injection_hashtags.items():
        cib[hasthtag] = int(base_rate * factor)

    # now create the bags of possible tweets for each condition
    cib_bag = [e for hashtag, count in cib.items() for e in repeat(hashtag, count)]
    norm_bag = [e for hashtag, count in base.items() for e in repeat(hashtag, count)]

    # create samples by day
    random.seed(54321)

    n_days = 21  # say, we have 21 days of data
    n_tweets = [random.randint(min_tweets, max_tweets) for _ in range(n_days)]

    # after day 10 the cib behavior kicks in
    tweets = [random.sample(norm_bag, n) for n in n_tweets[0:10]]
    tweets += [random.sample(cib_bag, n) for n in n_tweets[10::]]

    tweets_dict = {i: tweets[i] for i in range(n_days)}

    # create a vocabulary
    vocab = list(base.keys())

    return tweets_dict, vocab


# now get the counts and compute the gini coefficient
def gini_cib(tweets: dict, vocab: list):
    n_days = len(tweets)
    counts = {
        i: {hashtag: tweets[i].count(hashtag) for hashtag in vocab}
        for i in range(n_days)
    }
    ginis = {
        i: gini(
            list(counts[i].values())
        )
        for i in range(n_days)
    }

    return ginis, counts

def get_group_counts(counts: dict, vocab: list, injection_vocab:list):
    
    n_days = len(counts)
    other_hashtags = [hashtag for hashtag in vocab if hashtag not in injection_vocab]
    count_out = {
        i: {
            "Mango tweets": sum([counts[i][hashtag] for hashtag in injection_vocab]),
            "Other tweets": sum([counts[i][hashtag] for hashtag in other_hashtags])
        }
        for i in range(n_days)
    }

    return count_out


def plot_ginis(ginis: dict, group_counts: dict):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(2, 1, figsize=(8, 4.5), sharex=True, layout="constrained")
    xlabels = [f"Day {i+1}" for i in ginis.keys()]
    ax[0].plot(xlabels, ginis.values(), linestyle="--", marker="o")
    ax[0].set_ylabel("Gini coefficient")

    # add vertical line at day 11
    ax[0].axvline(x=10, color="tab:red", linestyle="--", label="CIB behavior starts")
    ax[0].legend()

    # stacked bar chart
    x = list(group_counts.keys())
    y = list(group_counts.values())
    y1 = [e["Other tweets"] for e in y]
    y2 = [e["Mango tweets"] for e in y]

    ax[1].bar(x, y1, label="Other hashtags")
    ax[1].bar(x, y2, bottom=y1, label="MangoTree hashtags")
    ax[1].set_title("Actual tweet distribution by group")
    ax[1].set_ylabel("Count")
    ax[1].legend()

    ax[1].tick_params(axis="x", rotation=45)

    for a in ax:
        a.grid(visible=True, linestyle="--", alpha=0.5)
        a.spines["top"].set_visible(False)
        a.spines['right'].set_visible(False)

    return fig