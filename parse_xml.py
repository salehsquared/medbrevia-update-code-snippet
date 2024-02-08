import calendar
from typing import List
import xml.etree.ElementTree as ET

# In case there are sub-texts (e.g. <x>y</x>) within the <AbstractText> xml.
def get_all_text(elem):
    text = elem.text or ""
    for subelem in elem:
        text += ET.tostring(subelem, encoding='unicode', method='text')
    text += elem.tail or ""
    return text

# date_type='pubmed' -> publish date
# date_type='accepted' -> accepted date
def get_date(pubmed_article, date_type='pubmed'):
    pubmed_date_element = pubmed_article.find(f".//PubMedPubDate[@PubStatus='{date_type}']")
    
    try:
        year = int(pubmed_date_element.find('Year').text)
    except:
        return ''

    try:
        month = int(pubmed_date_element.find('Month').text)
    except:
        month = 1  # default to January if month is missing

    try:
        day = int(pubmed_date_element.find('Day').text)
    except:
        day = 1  # default to the first day if day is missing

    # Check if the day is valid for the given month and year
    last_day_of_month = calendar.monthrange(year, month)[1]
    if day > last_day_of_month:
        day = last_day_of_month  # adjust day to the last day of the month

    return f"{year:04d}-{month:02d}-{day:02d}"

def get_authors(pubmed_article):
    authors = []
    for author in pubmed_article.findall('.//Author'):
        # Extracting author's name
        last_name = author.find('LastName').text if author.find('LastName') is not None else ''
        fore_name = author.find('ForeName').text if author.find('ForeName') is not None else ''
        name = f"{fore_name} {last_name}".strip()

        # Extracting author's affiliations
        # affiliations = [aff.text for aff in author.findall('.//AffiliationInfo/Affiliation')]

        authors.append({'name': name, 'affiliations': []})
    return authors

def get_keywords(pubmed_article):
    keywords = []
    for keyword in pubmed_article.findall('.//KeywordList/Keyword'):
        keyword_text = keyword.text
        major_topic = keyword.attrib.get('MajorTopicYN', 'N') == 'Y'
        keywords.append({'majorTopic': major_topic, 'name': keyword_text})
    return keywords

def get_grants(pubmed_article):
    grants = []
    for grant in pubmed_article.findall('.//GrantList/Grant'):
        grant_id = grant.find('GrantID').text if grant.find('GrantID') is not None else ''
        country = grant.find('Country').text if grant.find('Country') is not None else ''
        agency = grant.find('Agency').text if grant.find('Agency') is not None else ''
        if grant_id or country or agency:
            grants.append({'grant_id': grant_id, 'grant_country': country, 'agency': agency})
    return grants

def get_mesh_headings(pubmed_article):
    mesh_headings = []
    for mesh_heading in pubmed_article.findall('.//MeshHeadingList/MeshHeading'):
        heading_name = mesh_heading.find('DescriptorName').text
        major_topic = mesh_heading.find('DescriptorName').attrib.get('MajorTopicYN', 'N') == 'Y'
        if heading_name:
            mesh_headings.append({'majorTopic': major_topic, 'name': heading_name})
    return mesh_headings

def get_references(pubmed_article):
    references = []
    for reference in pubmed_article.findall('.//ReferenceList/Reference'):
        ref_name = reference.find('Citation').text if reference.find('Citation') is not None else ''
        pubmed_id_elem = reference.find(".//ArticleIdList/ArticleId[@IdType='pubmed']")
        pubmed_id = pubmed_id_elem.text if pubmed_id_elem is not None else None
        references.append({'name': ref_name, 'pubmed_id': pubmed_id})
    return references

def get_chemicals(pubmed_article):
    chemicals = []
    for chemical in pubmed_article.findall('.//ChemicalList/Chemical'):
        substance_name = chemical.find('NameOfSubstance').text if chemical.find('NameOfSubstance') is not None else ''
        registry_number = chemical.find('RegistryNumber').text if chemical.find('RegistryNumber') is not None else ''
        chemicals.append({'name': substance_name, 'registry_number': registry_number})
    return chemicals


def parse_article(pubmed_article):
    try: 
        d = {
            'pubmed_id': pubmed_article.find('.//PMID').text,
            'title': get_all_text(pubmed_article.find('.//ArticleTitle')).strip(),
        }
    except:
        return {}
    
    d['pub_date'] = get_date(pubmed_article)
    if not d['pub_date']:
        return {}
    
    if get_date(pubmed_article, date_type='accepted'):
        d['accepted_date'] = get_date(pubmed_article, date_type='accepted')

    if get_date(pubmed_article, date_type='received'):
        d['received_date'] = get_date(pubmed_article, date_type='received')

    authors = get_authors(pubmed_article)
    if authors:
        d['authors'] = authors

    keywords = get_keywords(pubmed_article)
    if keywords:
        d['keywords'] = keywords

    grants = get_grants(pubmed_article)
    if grants:
        d['grants'] = grants

    # references = get_references(pubmed_article)
    # if references:
    #     d['references'] = references

    mesh_headings = get_mesh_headings(pubmed_article)
    if mesh_headings:
        d['mesh_headings'] = mesh_headings

    try:
        # Sometimes, there are multiple <AbstractText>
        d['abstract'] = ' '.join([get_all_text(x).strip() for x in pubmed_article.findall('.//AbstractText')])
    except:
        pass

    try:
        d['doi'] = pubmed_article.find(".//ArticleId[@IdType='doi']").text
    except:
        pass

    d['journal'] = get_all_text(pubmed_article.find('.//Journal/Title')).strip()
    d['nlm_unique_id'] = pubmed_article.find('.//NlmUniqueID').text


    publication_types = [pub_type.text for pub_type in pubmed_article.findall(".//PublicationType")]
    d['pub_types'] = publication_types
    return d

def process_xml_to_article_list(xml_file: str) -> List[dict]:
    tree = ET.parse(xml_file)
    root = tree.getroot()

    article_list = []

    ct_passed = 0
    ct = 0
    for pubmed_article in root.findall('PubmedArticle'):
        article = parse_article(pubmed_article)
        if article:
            ct += 1
        else:
            ct_passed += 1
        article_list.append(article)
    return article_list