import re


class TitanSensor:
    def __init__(self):
        # DIZIONARI DI PESO (TITAN-10 PROTOCOL)

        # KINETIC (K): Violenza fisica e scala militare
        self.k_terms = {
            'nuclear': 1.0, 'ballistic': 0.9, 'thermobaric': 0.9,
            'breakthrough': 0.8, 'encirclement': 0.8, 'offensive': 0.7,
            'airstrike': 0.7, 'missile': 0.7, 'barrage': 0.6,
            'shelling': 0.5, 'clash': 0.4, 'skirmish': 0.3,
            'shooting': 0.3, 'intercepted': 0.2
        }

        # TEMPO (T): Urgenza e velocità
        self.t_terms = {
            'imminent': 1.0, 'breaking': 0.9, 'now': 0.9,
            'underway': 0.8, 'ongoing': 0.8, 'immediate': 0.8,
            'rapid': 0.7, 'tonight': 0.6, 'alert': 0.6,
            'reported': 0.4, 'yesterday': 0.2, 'past': 0.1
        }

        # EFFECT (E): Impatto sistemico
        self.e_terms = {
            'collapse': 1.0, 'grid down': 0.9, 'blackout': 0.9,
            'mass casualty': 0.9, 'critical': 0.8, 'infrastructure': 0.7,
            'bridge': 0.7, 'depot': 0.6, 'panic': 0.5,
            'evacuation': 0.5, 'damage': 0.4, 'disruption': 0.3
        }

    def calculate_metric(self, text, term_dict):
        """Calcola una metrica normalizzata (0.0 - 1.0) basata sulla presenza e peso dei termini"""
        score = 0.0
        text = text.lower()
        matches = 0

        for term, weight in term_dict.items():
            # Regex per trovare la parola esatta, non sottostringhe (es. evitare 'now' in 'know')
            if re.search(r'\b' + re.escape(term) + r'\b', text):
                score += weight
                matches += 1

        # Logica di saturazione: più termini trovi, più sale, ma non linearmente (diminishing returns)
        # Se trovi 1 termine forte (0.9) -> score 0.9
        # Se trovi 3 termini medi -> score accumulato ma cappato a 1.0
        return min(score, 1.0)

    def analyze_text(self, text):
        """
        Input: Raw Text
        Output: T.I.E. Score Completo
        """
        if not text:
            return {"tie_score": 0.0, "k": 0, "t": 0, "e": 0}

        # 1. Misurazione Dimensionale
        k_val = self.calculate_metric(text, self.k_terms)
        t_val = self.calculate_metric(text, self.t_terms)
        e_val = self.calculate_metric(text, self.e_terms)

        # 2. Formula T.I.E. (Weighted Average)
        # Kinetic conta il 50%, Tempo il 30%, Effect il 20%
        # Questa formula riflette la dottrina militare: la violenza (K) è dominante,
        # ma l'urgenza (T) è un moltiplicatore di rischio.
        raw_tie = (k_val * 50) + (t_val * 30) + (e_val * 20)

        # 3. Boost Multipliers (Logica Combinatoria)
        # Se è Cinetico E Urgente (Attacco in corso), boost del 20%
        if k_val > 0.6 and t_val > 0.6:
            raw_tie *= 1.2

        final_tie = min(round(raw_tie, 2), 100.0)

        return {
            "tie_score": final_tie,
            "k_metric": round(k_val, 2),
            "t_metric": round(t_val, 2),
            "e_metric": round(e_val, 2)
        }
