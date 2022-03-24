#!/usr/bin/env python3


mail_recipients = ['djiby.toure@fnacdarty.com', 'julien.pichot@fnacdarty.com', 'katell.bergot@fnacdarty.com']
# mail_recipients = ['sophie.nassour-ext@fnacdarty.com']
cc_mail_recipients = ['sophie.nassour-ext@fnacdarty.com']

project_id = 436828
spider_names = ['apple_occ',
                'murfy_occ',
                # 'fnac_occ',
                'recommerce_occ',
                'backmarket_occ',
                # 'yesyes_occ',
                'amazon_occ',
                'certideal_occ']
                # 'rebuy_occ']
subject = 'Crawl seconde vie'
final_filename = 'crawl_occasions_reconditionne'


def get_filtered_line(filtered_ean=None):
    if filtered_ean:
        return f'''<br /><br />Nombre d'EAN filtrés : {filtered_ean}'''
    return ''


def message_text(today_dmy, filtered_ean=None):
    filtered_line = get_filtered_line(filtered_ean)
    return f'''<p>Bonjour,\n \
    <br /><br />Le résultat du crawl Occasions / Reconditionné du {today_dmy} est à votre disposition en pièce(s) jointe(s).\
    {filtered_line}\
    <br /><br />— Ceci est un message envoyé automatiquement.</p>'''
