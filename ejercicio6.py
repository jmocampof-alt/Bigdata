from mrjob.job import MRJob

class VarianzaVotos(MRJob):

    def mapper(self, _, linea):
        partes = linea.strip().split(',')
        if len(partes) == 3:
            candidato = partes[0]
            votos = int(partes[2])
            yield candidato, (votos, votos**2, 1)

    def reducer(self, candidato, valores):
        suma = 0
        suma_cuadrados = 0
        conteo = 0

        for v, v2, c in valores:
            suma += v
            suma_cuadrados += v2
            conteo += c

        varianza = (suma_cuadrados / conteo) - (suma / conteo)**2
        yield candidato, round(varianza, 2)

if __name__ == '__main__':
    VarianzaVotos.run()
