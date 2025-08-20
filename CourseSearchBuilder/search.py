import logging
import requests
import json
import os

from SharedCode import exceptions
from . import models


def build_index(url, api_key, api_version, version):
    index = Index(url, api_key, api_version, version)

    index.delete_if_already_exists()
    index.create()


def load_index(url, api_key, api_version, version, docs):
    load = Load(url, api_key, api_version, version, docs)

    load.course_documents()


def build_synonyms(url, api_key, api_version):
    synonyms = SynonymMap(url, api_key, api_version)

    synonyms.update()


class Index:
    """Creates a new index"""

    def __init__(self, url, api_key, api_version, version):

        self.query_string = "?api-version=" + api_version
        self.index_name = f"courses-{version}"
        self.url = url

        self.headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
            "odata": "verbose",
        }

    def delete_if_already_exists(self):

        try:
            delete_url = self.url + "/indexes/" + self.index_name + self.query_string

            response = requests.delete(delete_url, headers=self.headers)

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error deleting index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code == 204:
            logging.warning(
                f"course search index already exists, successful deletion\
                           prior to recreation\n index: {self.index_name}"
            )

        elif response.status_code != 404:
            # 404 is the expected response, because normally the
            # course search index will not exist
            logging.error(
                f"unexpected response when deleting existing search index,\
                            search_response: {response.json()}\nindex-name:\
                            {self.index_name}\nstatus: {response.status_code}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def create(self):
        self.get_index()

        try:
            create_url = self.url + "/indexes" + self.query_string
            response = requests.post(
                create_url, headers=self.headers, json=self.course_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error creating index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(
                f"failed to create search index\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def get_index(self):
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, "schemas/course.json")) as json_file:
            schema = json.load(json_file)
            schema["name"] = self.index_name

            self.course_schema = schema


class Load:
    """Loads course documents into search index"""

    def __init__(self, url, api_key, api_version, version, docs):

        self.url = url
        self.headers = {
            "Content-Type": "application/json",
            "api-key": api_key,
            "odata": "verbose",
        }
        self.query_string = "?api-version=" + api_version
        self.index_name = f"courses-{version}"

        self.docs = docs

    def course_documents(self):

        number_of_docs = len(self.docs)
        logging.info(f"THERE ARE A TOTAL OF {number_of_docs} courses")
        course_count = 0
        bulk_course_count = 500
        documents = {}
        search_courses = []
        for doc in self.docs:
            course_count += 1

            search_course = models.build_course_search_doc(doc)
            search_courses.append(search_course)

            if course_count % bulk_course_count == 0 or course_count == number_of_docs:

                documents["value"] = search_courses

                self.bulk_create_courses(documents)

                logging.info(
                    f"successfully loaded {course_count} courses into azure search\n\
                        index: {self.index_name}\n"
                )

                # Empty variables
                documents = {}
                search_courses = []

    def bulk_create_courses(self, documents):

        try:
            url = (
                self.url
                + "/indexes/"
                + self.index_name
                + "/docs/index"
                + self.query_string
            )
            logging.info(f"url: {url}")
            response = requests.post(url, headers=self.headers, json=documents)
        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error loading bulk index", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 200:
            logging.error(
                f"failed to bulk load course search documents\n\
                            index-name: {self.index_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException()


class SynonymMap:
    """Creates a new synonym"""

    # TODO review this code and tidy up where needed when time permits

    def __init__(self, url, api_key, api_version):

        self.query_string = "?api-version=" + api_version
        self.synonym_name = "english-course-title"
        self.url = url

        self.headers = {"Content-Type": "application/json", "api-key": api_key}

    def update(self):
        self.get_synonym()

        try:
            update_url = (
                self.url + "/synonymmaps/" + self.synonym_name + self.query_string
            )
            response = requests.put(
                update_url, headers=self.headers, json=self.course_synonym_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error creating course synonym", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code == 201:
            # PUT can return 201 if a new resource is created
            return

        if response.status_code == 204:
            return

        if response.status_code == 404:
            logging.warning(
                f"failed to update course search synonyms, unable to find synonym; try creating synonym\n\
                            synonym-name: {self.synonym_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            self.create()
        else:
            logging.error(
                f"failed to update course search synonyms\n\
                            synonym-name: {self.synonym_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def create(self):
        try:
            create_url = self.url + "/synonymmaps" + self.query_string
            response = requests.put(
                create_url, headers=self.headers, json=self.course_synonym_schema
            )

        except requests.exceptions.RequestException as e:
            logging.exception("unexpected error creating course synonym", exc_info=True)
            raise exceptions.StopEtlPipelineErrorException(e)

        if response.status_code != 201:
            logging.error(
                f"failed to create course search synonyms, after failing to update synonyms\n\
                            synonym-name: {self.synonym_name}\n\
                            status: {response.status_code}\n\
                            error: {requests.exceptions.HTTPError(response.text)}"
            )

            raise exceptions.StopEtlPipelineErrorException

    def get_synonym(self):
        cwd = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(cwd, "schemas/course_synonym.json")) as json_file:
            schema = json.load(json_file)
            schema["name"] = self.synonym_name
            schema["synonyms"] = self.get_synonym_list()

            self.course_synonym_schema = schema

    def get_synonym_list(self):
        # Medicine & Dentistry
        medicine_synonyms = "medicine, medical degree, MBBS, MBChB, BMBS, clinical medicine, medical science, surgery, physician, doctor, general practice, GP, internal medicine => medicine"
        dentist_synonyms = "dentist, dentistry, BDS, dental surgery, oral science, endodontics, orthodontics, dentofacial orthopedics, oral medicine, oral surgery, maxillofacial surgery, paediatric dentistry, pediatric dentistry, public health dentistry, prosthodontics, prosthodontist, tooth doctor, dental surgeon, dental practitioner, dental hygienist, hygienist, clinical dentistry, dental therapy => dentistry"

        # Nursing & Midwifery
        midwifery_synonyms = "midwifery, midwife, obstetrics, obstetrical delivery, perinatal, perinatology, fetology, foetology, tocology, maternity care, childbirth, neonatal care, labour ward, delivery suite, postnatal care, antenatal care => midwifery"
        adult_nursing_synonyms = "adult nursing, registered nurse adult (RNA), general nursing, medical nursing, surgical nursing, adult health, nursing care, palliative care, district nursing => adult nursing"
        mental_health_nursing_synonyms = "mental health nursing, registered mental health nursing (RMN), mental health nurse, psychiatric nursing, psychiatric nurse, behavioural health, psychiatric care, psychological nursing, dual diagnosis, forensic mental health, community mental health => mental health nursing"
        dental_nursing_synonyms = "dental nursing, dental nurse, dental hygienist, dental practitioner, oral health care, dental assistant, dental surgery assistant, DSA, orthodontic nursing => dental nursing"

        # Pharmacology & Pharmacy
        pharmacology_synonyms = "pharmacology, psychopharmacology, pharmacological medicine, pharmacokinetics, pharmacodynamics, clinical pharmacology, molecular pharmacology, drug action, medicinal chemistry, pharmaceutical science, drug discovery, pharmacotherapeutics => pharmacology"
        toxicology_synonyms = "toxicology, poison science, environmental toxicology, clinical toxicology, forensic toxicology, industrial toxicology, regulatory toxicology, ecotoxicology, toxinology, chemical safety, hazard assessment => toxicology"
        pharmacy_synonyms = "pharmacy, MPharm, pharmacist, chemist, pharmaceutical science, pharmacy practice, clinical pharmacy, community pharmacy, hospital pharmacy, industrial pharmacy, prescribing, medicines management, pharmacotherapy, pharmaceutics, dispensing => pharmacy"

        # Sciences
        chemistry_synonyms = "chemistry, chemical science, inorganic chemistry, surface chemistry, geochemistry, natural science, radiochemistry, thermochemistry, organic chemistry, physical chemistry, photochemistry, chemoimmunology, immunochemistry, femtochemistry, analytical chemistry, environmental chemistry, medicinal chemistry, materials chemistry, computational chemistry, forensic chemistry => chemistry"
        biology_synonyms = "biology, biological science, life science, molecular biology, cell biology, genetics, genomics, microbiology, marine biology, zoology, ecology, conservation biology, evolutionary biology, human biology, biotechnology, biomedical science => biology"
        physics_synonyms = "physics, physical science, astrophysics, quantum physics, particle physics, theoretical physics, applied physics, medical physics, geophysics, nuclear physics, thermodynamics, condensed matter physics, astrophysics => physics"

        # Mathematics & Computing
        mathematics_synonyms = "mathematics, maths, math, applied mathematics, pure maths, pure math, pure mathematics, applied maths, applied math, mathematical science, operational research, OR, data science, analytics, quantitative methods, mathematical modelling, computational mathematics, statistics, stat, stats, statistic => mathematics"
        computer_science_synonyms = "computer science, computing, comp sci, software engineering, informatics, artificial intelligence, AI, machine learning, data science, computer systems, networking, cybersecurity, cyber security, IT, information technology, programming, software development => computer science"

        # Engineering & Technology
        civil_engineering_synonyms = "civil engineering, construction engineering, structural engineering, transportation engineering, geotechnical engineering, environmental engineering, water engineering, infrastructure engineering, roads, bridges, tunnels, built environment, applied science, engineering science, hydraulic engineering, hydrolic engineering, public works engineering, municipal engineering => civil engineering"
        mechanical_engineering_synonyms = "mechanical engineering, mech eng, automotive engineering, aerospace engineering, thermofluids, dynamics, mechanics, manufacturing engineering, materials engineering, robotics, mechatronics, industrial engineering => mechanical engineering"
        electrical_engineering_synonyms = "electrical engineering, electronic engineering, electronics, EEE, electrical and electronic engineering, power systems, microelectronics, telecommunications, telecoms, control systems, robotics => electrical engineering"

        # Business, Economics & Law
        business_synonyms = "business, business studies, business administration, management, business management, international business, entrepreneurship, enterprise, leadership, strategy, marketing, human resources, HR, finance, accounting, commerce => business"
        economics_synonyms = "economics, econ, econometrics, microeconomics, macroeconomics, financial economics, development economics, behavioural economics, economic policy, political economy => economics"
        law_synonyms = "law, LLB, jurisprudence, legal studies, commercial law, criminal law, international law, human rights law, corporate law, solicitor, barrister, legal practice => law"

        # Arts, Humanities & Social Sciences
        psychology_synonyms = "psychology, psych, clinical psychology, cognitive psychology, developmental psychology, social psychology, forensic psychology, neuropsychology, counselling psychology, psychotherapy, behavioural science => psychology"
        sociology_synonyms = "sociology, social studies, social policy, criminology, social work, social care, community studies, inequality, social theory, cultural studies => sociology"
        history_synonyms = "history, historical studies, modern history, ancient history, medieval history, economic history, art history, public history, heritage studies => history"
        english_literature_synonyms = "english, english literature, literature, literary studies, creative writing, poetry, prose, drama, Shakespeare, literary criticism, comparative literature => english literature"

        # Creative Arts & Design
        art_design_synonyms = "art, design, fine art, graphic design, illustration, fashion design, textile design, interior design, product design, industrial design, 3D design, animation, visual communication, creative arts, digital arts => art and design"
        architecture_synonyms = "architecture, architectural studies, built environment, urban design, landscape architecture, interior architecture, architectural technology, RIBA, sustainable design => architecture"
        music_synonyms = "music, music performance, music composition, music technology, music production, sound engineering, musicology, ethnomusicology, musical theatre, jazz, classical music => music"

        # Education
        education_synonyms = "education, teaching, teacher training, PGCE, pedagogy, educational studies, childhood studies, early years, primary education, secondary education, further education, educational leadership, special educational needs, SEN => education"

        # Combined & Interdisciplinary
        biomedical_science_synonyms = "biomedical science, biomedical, medical science, pathology, physiology, pharmacology, immunology, hematology, haematology, clinical science, virology => biomedical science"
        environmental_science_synonyms = "environmental science, environmental studies, sustainability, climate change, ecology, conservation, geography, earth science, geoscience, natural resources, renewable energy => environmental science"

        synonyms = {
            medicine_synonyms,
            dentist_synonyms,
            midwifery_synonyms,
            adult_nursing_synonyms,
            mental_health_nursing_synonyms,
            dental_nursing_synonyms,
            pharmacology_synonyms,
            toxicology_synonyms,
            pharmacy_synonyms,
            chemistry_synonyms,
            biology_synonyms,
            physics_synonyms,
            mathematics_synonyms,
            computer_science_synonyms,
            civil_engineering_synonyms,
            mechanical_engineering_synonyms,
            electrical_engineering_synonyms,
            business_synonyms,
            economics_synonyms,
            law_synonyms,
            psychology_synonyms,
            sociology_synonyms,
            history_synonyms,
            english_literature_synonyms,
            art_design_synonyms,
            architecture_synonyms,
            music_synonyms,
            education_synonyms,
            biomedical_science_synonyms,
            environmental_science_synonyms
        }

        return "\n".join(synonyms)