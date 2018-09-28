QUERIES = { 
#SisIntId,SisExtId,FirstName,MiddleName,LastName,Suffix,Sex,Ethnicity,ZipCode,USResidency,HsGpa,ColGpaCum,ActiveDuty,Veteran,EduLevelPaternal,EduLevelMaternal,EduLevelParental,EnrollmentLevel,CourseCount,SatMathPre2016,SatMathPost2016,SatMathCombined,SatVerbalPre2016,SatReadingPost2016,SatVerbalReadingCombined,SatWritingPre2016,SatWritingPost2016,SatWritingCombined,SatTotalCombined,ActReading,ActMath,ActEnglish,ActScience,ActComposite,PhoneNumber,PhoneType,EmailAddress,EmailType
# CourseCount and Prefix are not present in SIS, currently removed
  'person' : {
    'index' : 'sisintid',
    'sis_file' : '{date}%2Fperson_{date}.csv',
    # Setup a temporary table for this, this is much faster than using a WITH because of the index
    'prequery' : """
        CREATE TEMPORARY TABLE CourseCountValues AS (SELECT PersonId, COUNT(*) AS CourseCount FROM OrganizationPersonRole GROUP BY PersonId);
        CREATE INDEX course_count_person_id_index ON CourseCountValues(PersonId);
        
    """,
    'query' : """
            SELECT
              ucdmint.sourcekey as SisIntId,
              ucdmext.sourcekey as SisExtId,
              Person.Firstname as FirstName,
              Person.Middlename as MiddleName,
              Person.Lastname as LastName,
              --Person.Prefix as Prefix,
              Person.GenerationCode as Suffix,
              RefSex.Code as Sex,
              RefRace.Code as Ethnicity,
              PersonAddress.PostalCode as ZipCode,
              RefUSCitizenshipStatus.Code as UsResidency,
              (SELECT PsStudentApplication.GradePointAverageCumulative
                FROM PsStudentApplication
                LEFT JOIN OrganizationPersonRole on PsStudentApplication.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7  
              ) as HsGpa,
              (SELECT PsStudentAcademicRecord.GradePointAverageCumulative
                FROM PsStudentAcademicRecord
                LEFT JOIN OrganizationPersonRole on PsStudentAcademicRecord.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7  
              ) as ColGpaCum,              
              RefMilitaryActiveStudentIndicator.Code as ActiveDuty,
              RefMilitaryVeteranStudentIndicator.Code as Veteran,
              (SELECT RefEducationLevel.Code
                FROM PsStudentDemographic
                LEFT JOIN OrganizationPersonRole ON PsStudentDemoGraphic.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefEducationLevel ON PsStudentDemographic.RefPaternalEducationLevelId = RefEducationLevel.RefEducationLevelId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7  
              ) as EduLevelPaternal,      
              (SELECT RefEducationLevel.Code
                FROM PsStudentDemographic
                LEFT JOIN OrganizationPersonRole ON PsStudentDemoGraphic.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefEducationLevel ON PsStudentDemographic.RefMaternalEducationLevelId = RefEducationLevel.RefEducationLevelId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7  
              ) as EduLevelMaternal,
              (SELECT RefEducationLevel.Code
                FROM PsStudentDemographic
                LEFT JOIN OrganizationPersonRole ON PsStudentDemoGraphic.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefEducationLevel ON PsStudentDemographic.RefParentalEducationLevelId = RefEducationLevel.RefEducationLevelId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7  
              ) as EduLevelParental,
              (SELECT RefPsStudentLevel.Code
                FROM PsStudentEnrollment
                LEFT JOIN OrganizationPersonRole ON PsStudentEnrollment.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefPsStudentLevel ON PsStudentEnrollment.RefPsStudentLevelId = RefPsStudentLevel.RefPsStudentLevelId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7  
              ) as EnrollmentLevel,
--              (SELECT CourseCount from CourseCountValues where Person.PersonId = CourseCountValues.PersonId) as CourseCount,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatMathPre2016'
              ) as SatMathPre2016,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatMathPost2016'
              ) as SatMathPost2016,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatMathCombined'
              ) as SatMathCombined,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatVerbalPre2016'
              ) as SatVerbalPre2016,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatReadingPost2016'
              ) as SatReadingPost2016,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatVerbalReadingCombined'
              ) as SatVerbalReadingCombined,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatWritingPre2016'
              ) as SatWritingPre2016,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatWritingPost2016'
              ) as SatWritingPost2016,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatWritingCombined'
              ) as SatWritingCombined,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='SatTotalCombined'
              ) as SatTotalCombined,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='ActReading'
              ) as ActReading,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='ActMath'
              ) as ActMath,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='ActEnglish'
              ) as ActEnglish,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='ActScience'
              ) as ActScience,
              (SELECT PsStudentAdmissionTest.StandardizedAdmissionTestScore
                FROM PsStudentAdmissionTest
                LEFT JOIN OrganizationPersonRole on PsStudentAdmissionTest.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
                LEFT JOIN RefStandardizedAdmissionTest on RefStandardizedAdmissionTest.RefStandardizedAdmissionTestId=PsStudentAdmissionTest.RefStandardizedAdmissionTestId
                WHERE
                  Person.PersonId = OrganizationPersonRole.PersonId AND
                  OrganizationPersonRole.OrganizationId=7 AND
                  RefStandardizedAdmissionTest.code='ActComposite'
              ) as ActComposite,
              PersonTelephone.TelephoneNumber as PhoneNumber,
              RefPersonTelephoneNumberType.Code as PhoneType,
              PersonEmailAddress.EmailAddress as EmailAddress,
              RefEmailType.Code as EmailType
            FROM Person
              LEFT JOIN RefSex on RefSex.RefSexId=Person.RefSexId
              LEFT JOIN PersonDemographicRace on PersonDemographicRace.PersonId=Person.PersonId
              LEFT JOIN RefRace on RefRace.RefRaceId=PersonDemographicRace.RefRaceId
              LEFT JOIN PersonAddress on PersonAddress.PersonId=Person.PersonId
              LEFT JOIN RefUSCitizenshipStatus on RefUSCitizenshipStatus.RefUSCitizenshipStatusId=Person.RefUSCitizenshipStatusId
              LEFT JOIN PersonMilitary on PersonMilitary.PersonId=Person.PersonId
              LEFT JOIN RefMilitaryActiveStudentIndicator on RefMilitaryActiveStudentIndicator.RefMilitaryActiveStudentIndicatorId=PersonMilitary.RefMilitaryActiveStudentIndicatorId
              LEFT JOIN RefMilitaryVeteranStudentIndicator on RefMilitaryVeteranStudentIndicator.RefMilitaryVeteranStudentIndicatorId=PersonMilitary.RefMilitaryVeteranStudentIndicatorId
              LEFT JOIN PersonEmailAddress on PersonEmailAddress.PersonId=Person.PersonId
              LEFT JOIN RefEmailType on RefEmailType.RefEmailTypeId=PersonEmailAddress.RefEmailTypeId
              LEFT JOIN PersonTelephone on PersonTelephone.PersonId=Person.PersonId
              LEFT JOIN RefPersonTelephoneNumberType on RefPersonTelephoneNumberType.RefPersonTelephoneNumberTypeId=PersonTelephone.RefPersonTelephoneNumberTypeId
              LEFT JOIN ucdmentitykeymap ucdmint on ucdmint.ucdmkey = Person.PersonId and ucdmint.ucdmentityid = 1 and ucdmint.systemprovisioningid = 1000
              LEFT JOIN ucdmentitykeymap ucdmext on ucdmext.ucdmkey = Person.PersonId and ucdmext.ucdmentityid = 1 and ucdmext.systemprovisioningid = 1001
  """},
  #SisIntId,SisExtId,TermId,CourseSubj,CourseNo,Title,Description,Status,AvailableCredits
  'course_offering' : {
    'index' : 'sisintid',
    'sis_file' : '{date}%2Fcourse_offering_{date}.csv',
    'query' : """
            SELECT
              ucdmint.sourcekey as SisIntId,
              ucdmext.sourcekey as SisExtId,
              PsCourse.OrganizationCalendarSessionId as TermId,
              Course.SubjectAbbreviation as CourseSubj,
              PsCourse.CourseNumber as CourseNo,
              PsCourse.CourseTitle as Title,
              Course.Description as Description,
              RefWorkflowState.Code as Status,
              Course.CreditValue as AvailableCredits,
              OrganizationCalendarSession.SessionName,
              RefSessionType.Code as SessionType
            FROM Course
              LEFT JOIN PsCourse on PsCourse.OrganizationId=Course.OrganizationId
              LEFT JOIN OrganizationCalendarSession on OrganizationCalendarSession.OrganizationCalendarSessionId=PsCourse.OrganizationCalendarSessionId
              LEFT JOIN RefCourseCreditUnit on RefCourseCreditUnit.RefCourseCreditUnitId=Course.RefCourseCreditUnitId
              LEFT JOIN RefWorkflowState on RefWorkflowState.RefWorkflowStateId=Course.RefWorkflowStateId
              LEFT JOIN RefSessionType on RefSessionType.RefSessionTypeId=OrganizationCalendarSession.RefSessionTypeId
              LEFT JOIN ucdmentitykeymap ucdmint on ucdmint.ucdmkey = Course.OrganizationId and ucdmint.ucdmentityid = 4 and ucdmint.systemprovisioningid = 1000
              LEFT JOIN ucdmentitykeymap ucdmext on ucdmext.ucdmkey = Course.OrganizationId and ucdmext.ucdmentityid = 4 and ucdmext.systemprovisioningid = 1001
  """},
  #SisIntId,SisExtId,CourseId,TermId,SectionNumber,DeliveryMode,MaxEnrollment
  'course_section' : {
    'index' : 'sisintid',
    'sis_file' : '{date}%2Fcourse_section_{date}.csv',
    'query' : """
            SELECT
              ucdmint.sourcekey as SisIntId,
              ucdmext.sourcekey as SisExtId,
              CourseSection.CourseId as CourseId,
              CourseSection.OrganizationCalendarSessionId as TermId,
              PsSection.SectionNumber as SectionNumber,
              RefCourseSectionDeliveryMode.Code as DeliveryMode,
              CourseSection.MaximumCapacity as MaxEnrollment,
              Organization.Name as Name,
              OrganizationCalendarSession.BeginDate as BeginDate,
              OrganizationCalendarSession.EndDate as EndDate,
              RefWorkflowState.Description as WorkflowState
            FROM CourseSection
              LEFT JOIN Organization on Organization.OrganizationId=CourseSection.OrganizationId
              LEFT JOIN PsSection on PsSection.OrganizationId=CourseSection.OrganizationId
              LEFT JOIN RefCourseSectionDeliveryMode on RefCourseSectionDeliveryMode.RefCourseSectionDeliveryModeId=CourseSection.RefCourseSectionDeliveryModeId
              LEFT JOIN RefCreditTypeEarned on RefCreditTypeEarned.RefCreditTypeEarnedId=CourseSection.RefCreditTypeEarnedId
              LEFT JOIN RefWorkflowState on RefWorkflowState.RefWorkflowStateId=CourseSection.RefWorkflowStateId
              LEFT JOIN OrganizationCalendarSession on OrganizationCalendarSession.OrganizationCalendarSessionId=CourseSection.OrganizationCalendarSessionId
              LEFT JOIN ucdmentitykeymap ucdmint on ucdmint.ucdmkey = CourseSection.OrganizationId and ucdmint.ucdmentityid = 5 and ucdmint.systemprovisioningid = 1000
              LEFT JOIN ucdmentitykeymap ucdmext on ucdmext.ucdmkey = CourseSection.OrganizationId and ucdmext.ucdmentityid = 5 and ucdmext.systemprovisioningid = 1001

  """},
  #SisIntId,SisExtId,TermType,SessionType,SessionName,TermBeginDate,TermEndDate,InstrBeginDate,InstrEndDate
  'academic_term' : {
    'index' : 'sisintid',
    'sis_file' : '{date}%2Facademic_term_{date}.csv',
    'query' : """
            SELECT
              ucdmint.sourcekey as SisIntId,
              ucdmext.sourcekey as SisExtId,
              OrganizationCalendarSession.OrganizationCalendarSessionId as TermId,
              RefTermType.Code as TermType,
              RefSessionType.Code as SessionType,
              OrganizationCalendarSession.SessionName as SessionName,
              OrganizationCalendarSession.BeginDate as TermBeginDate,
              OrganizationCalendarSession.EndDate as TermEndDate,
              OrganizationCalendarSession.FirstInstructionDate as InstrBeginDate,
              OrganizationCalendarSession.LastInstructionDate as InstrEndDate
            FROM OrganizationCalendarSession
              LEFT JOIN RefSessionType on RefSessionType.RefSessionTypeId=OrganizationCalendarSession.RefSessionTypeId
              LEFT JOIN OrganizationCalendar on OrganizationCalendar.OrganizationCalendarId=OrganizationCalendarSession.OrganizationCalendarId
              LEFT JOIN RefTermType on OrganizationCalendar.RefTermTypeId=RefTermType.RefTermTypeId
              LEFT JOIN ucdmentitykeymap ucdmint on ucdmint.ucdmkey = OrganizationCalendarSession.OrganizationCalendarSessionId and ucdmint.ucdmentityid = 3 and ucdmint.systemprovisioningid = 1000
              LEFT JOIN ucdmentitykeymap ucdmext on ucdmext.ucdmkey = OrganizationCalendarSession.OrganizationCalendarSessionId and ucdmext.ucdmentityid = 3 and ucdmext.systemprovisioningid = 1001

  """},
  #PersonId,SectionId,Role,RoleStatus,EntryDate,ExitDate,CreditsTaken,CreditsEarned
  'course_section_enrollment' : {
    'index' : 'sectionid',
    'sis_file' : '{date}%2Fcourse_section_enrollment_{date}.csv',
    'query' : """
            SELECT
              ucdmint.sourcekey as SisIntId,
              ucdmext.sourcekey as SisExtId,
              OrganizationPersonRole.PersonId as PersonId,
              ucdmint.sourcekey as SectionId,
              Role.Name as Role,
              RefRoleStatus.Code as RoleStatus,
              to_char(OrganizationPersonRole.EntryDate,'YYYY-MM-DD') as EntryDate,
              to_char(OrganizationPersonRole.ExitDate,'YYYY-MM-DD') as ExitDate,
              PsStudentSection.NumberOfCreditsTaken as CreditsTaken,
              PsStudentSection.NumberOfCreditsEarned as CreditsEarned
              -- Completed at??
              -- RefWorkflowState.Description as WorkflowState
              -- Self-enrolled?
            FROM OrganizationPersonRole
              INNER JOIN Role on Role.RoleId=OrganizationPersonRole.RoleId
              INNER JOIN RoleStatus on RoleStatus.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
              LEFT JOIN RefRoleStatus on RefRoleStatus.RefRoleStatusId=RoleStatus.RefRoleStatusId
              LEFT JOIN PsStudentSection on PsStudentSection.OrganizationPersonRoleId=OrganizationPersonRole.OrganizationPersonRoleId
              -- INNER JOIN RefWorkflowState on RefWorkflowState.RefWorkflowStateId=OrganizationPersonRole.RefWorkflowStateId
              LEFT JOIN ucdmentitykeymap ucdmint on ucdmint.ucdmkey = OrganizationPersonRole.OrganizationId and ucdmint.ucdmentityid = 5 and ucdmint.systemprovisioningid = 1000
              LEFT JOIN ucdmentitykeymap ucdmext on ucdmext.ucdmkey = OrganizationPersonRole.OrganizationId and ucdmext.ucdmentityid = 5 and ucdmext.systemprovisioningid = 1001
  """},
  #PersonId,DirectoryBlock
  'institutional_affiliation' : {
    'index' : 'personid',
    'sis_file' : '{date}%2Finstitutional_affiliation_{date}.csv',
    'query' : """
              SELECT ucdmint.sourcekey AS PersonId, 
                RefDirectoryInformationBlockStatus.Code AS DirectoryBlock 
            FROM PsStudentEnrollment        
              LEFT JOIN RefDirectoryInformationBlockStatus ON RefDirectoryInformationBlockStatus.RefDirectoryInformationBlockStatusId = PsStudentEnrollment.RefDirectoryInformationBlockStatusId
              LEFT JOIN OrganizationPersonRole ON PsStudentEnrollment.OrganizationPersonRoleId = OrganizationPersonRole.OrganizationPersonRoleId
              LEFT JOIN ucdmentitykeymap ucdmint ON ucdmint.ucdmkey = OrganizationPersonRole.PersonId AND ucdmint.ucdmentityid = 1 AND ucdmint.systemprovisioningid = 1000
              LEFT JOIN ucdmentitykeymap ucdmext ON ucdmext.ucdmkey = OrganizationPersonRole.PersonId AND ucdmext.ucdmentityid = 1 AND ucdmext.systemprovisioningid = 1001
              -- We only want the entiries where it's blocked or remove
            WHERE PsStudentEnrollment.RefDirectoryInformationBlockStatusId != -1
  """},
}