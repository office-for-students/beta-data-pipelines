from SharedCode import utils


class SubjectCourseEnricher:
    """Handles enriching courses with UKRLP data"""

    def __init__(self):
        self.subject_lookups = utils.get_subject_lookups()

    def enrich_course(self, course):
        """Takes a course and enriches subject object with subject names"""

        subject_code = course['course']['subject']['code']
        course['course']['subject'] = self.get_subject_object(
            subject_code)

    def get_subject_object(self, code):
        """Returns a subject object containing code, english_name, welsh_name and level"""
        subject = {
            "code": code
        }

        if code not in self.subject_lookups:
            return subject

        level = self.subject_lookups[code].get("level", "")
        english_name = self.subject_lookups[code].get("english_name", "")
        welsh_name = self.subject_lookups[code].get("welsh_name", "")

        if level != "":
            subject["level"] = level

        if english_name != "":
            subject["english_name"] = english_name

        if welsh_name != "":
            subject["welsh_name"] = english_name

        return subject
        
