from classes import Students, Careers, Courses, Enrollments
from pymongo import MongoClient


uri = 'mongodb+srv://Winchester:eO7NcxKyKD23roKw@clusterpoo.dzqveej.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(uri)
db = client.informacion_estudiantes

class Dataprocess:

    def __init__(self, data):
        self.__data = data
    
    ###
    def create_careers(self):

        careers = db.careers
        carreras = []
        carreras_unicas = []
        estudiantes_carreras = []
        

        for i in self.__data:
            carreras.append(i.get('carrera'))
        for i in carreras:
            if i not in carreras_unicas:
                carreras_unicas.append(i)
        carreras.clear()

        for j in carreras_unicas:
            estudiantes_carreras.clear()
            for i in self.__data:
                if j == i.get('carrera'):
                    estudiantes_carreras.append(i.get('nombre_completo'))
            carrera = Careers.Careers('', j, estudiantes_carreras)
            career_doc = {'ID de la Carrera': carrera.id,
                      'Nombre de la Carrera': carrera.nombre,
                      'Estudiantes de la Carrera': estudiantes_carreras}
            careers.insert_one(career_doc)
    ### 
    def create_courses(self):

        courses = db.courses
        cursos = []
        cursos_unicos = []

        for i in self.__data:
            for j in i.get('cursos_aprobados'):
                cursos.append(j)
            for j in i.get('cursos_reprobados'):
                cursos.append(j)
        for h in cursos:
            if h not in cursos_unicos:
                cursos_unicos.append(h)
        for k in cursos_unicos:
            curso = Courses.Courses('', k)
            course_doc = {'ID del Curso': curso.id,
                          'Nombre del Curso': curso.nombre}
            courses.insert_one(course_doc)
    ###
    def create_students(self):
        
        students = db.students

        for i in self.__data:

            estudiante = Students.Students(i.get('numero_cuenta'), i.get('nombre_completo'), i.get('edad'), i.get('carrera'))
            student_doc = {'Número de Cuenta': estudiante.numeroDeCuenta,
                            'Nombre Completo': estudiante.nombreCompleto,
                            'Edad': estudiante.edad,
                            'Carrera': estudiante.carrera
                            }
            students.insert_one(student_doc)
    ###
    def create_enrollments(self):
         
        enrollments = db.enrollments
        inscripciones = []

        for i in self.__data:
            for j in i.get('cursos_aprobados'):
                inscripcion1 = Enrollments.Enrollments('', j, i.get('nombre_completo'), 'Aprobado')
                inscripciones.append(inscripcion1)
        for i in inscripciones:  
            enrollment_doc1 = {'ID de la inscripción': i.id,
                                'Curso': i.curso,
                                'Estudiante': i.estudiante,
                                'Estado': i.estado}
            enrollments.insert_one(enrollment_doc1)
        inscripciones.clear()
        for i in self.__data:
            for j in i.get('cursos_reprobados'):
                inscripcion2 = Enrollments.Enrollments('', j, i.get('nombre_completo'), 'Reprobado')
                inscripciones.append(inscripcion2)
        for i in inscripciones:
            enrollment_doc2 = {'ID de la inscripción': inscripcion2.id,
                                'Curso': inscripcion2.curso,
                                'Estudiante': inscripcion2.estudiante,
                                'Estado': inscripcion2.estado}
            enrollments.insert_one(enrollment_doc2)
                 
