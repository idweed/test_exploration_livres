# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'project',
    version      = '1.0',
    packages     = find_packages(),
    package_data ={
        'albert': ['data/*.csv', 'data/amazon_exploration_livres_ean/livres_ean_asin_final_dropna.csv']
    },
    # package_data ={
    #     'albert': ['data/*.csv', 'data/occasions', 'data/occasions/*.csv', 'data/boulanger_PEM/*.txt']
    # },
    scripts=['albert/launching_scripts/livraisons/boulanger_livraisons_script.py',
             'albert/launching_scripts/pricing/boulanger_darty_ean.py',
             'albert/launching_scripts/occasions/occasions.py'],
    entry_points = {'scrapy': ['settings = albert.settings']},
    zip_safe=False,
)
