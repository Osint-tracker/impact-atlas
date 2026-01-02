import json
import sqlite3
from datetime import datetime


class BehaviorAnalyst:
    def __init__(self, db_path='osint_tracker.db'):
        self.db_path = db_path

    def _get_historical_confidence(self, phase_type):
        """
        1. CONFIDENCE LEDGER (La Memoria)
        Interroga il DB: qual è la mia % di successo storica per questa fase?
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Cerca le ultime 20 previsioni verificate per questo tipo di fase
        query = """
        SELECT verification_status 
        FROM prediction_history 
        WHERE predicted_phase = ? AND verification_status != 'PENDING'
        ORDER BY created_at DESC LIMIT 20
        """
        cursor.execute(query, (phase_type,))
        results = cursor.fetchall()
        conn.close()

        if not results:
            return 0.50  # Default neutro se non ho storia (50%)

        # Calcolo semplice: (Successi) / (Totale Verificati)
        successes = sum(1 for r in results if r[0] == 'CONFIRMED')
        return round(successes / len(results), 2)

    def falsify_hypothesis(self, tie_score, active_signals_or_targets):
        """
        2. DEVIL'S ADVOCATE (Segnali Negativi)
        Accetta sia un dizionario di segnali che una lista di target per flessibilità.
        """
        falsification_score = 0.0
        reasons = []

        # Normalizziamo l'input: vogliamo lavorare con un dizionario o set per fare check veloci
        context_set = set()

        if isinstance(active_signals_or_targets, list):
            # Se arriva una lista ['ammo_depot', ...], la convertiamo in set
            context_set = set(active_signals_or_targets)
            # Creiamo un dict fittizio per compatibilità con la logica vecchia
            signals_dict = {k: True for k in active_signals_or_targets}
        elif isinstance(active_signals_or_targets, dict):
            signals_dict = active_signals_or_targets
            # Aggiungiamo le chiavi true al set
            context_set = {
                k for k, v in active_signals_or_targets.items() if v}
        else:
            signals_dict = {}

        # --- LOGICA DI FALSIFICAZIONE ---

        # Ipotesi: Stanno attaccando (High T.I.E.) MA...

        # Check A: Rotazione Truppe Inversa? (Segnale Negativo)
        if signals_dict.get('troop_withdrawal', False):
            falsification_score += 0.4
            reasons.append("Truppe in ritiro durante picco di attività")

        # Check B: Fortificazioni Difensive?
        if signals_dict.get('building_defenses', False):
            falsification_score += 0.3
            reasons.append("Costruzione difese (postura reattiva)")

        # Check C: Assenza Coordinazione Temporale (se passata come segnale)
        # Nota: Qui assumiamo che se 'temporal_coordination' è False nel dict, allora manca.
        if 'temporal_coordination' in signals_dict and not signals_dict['temporal_coordination']:
            falsification_score += 0.2
            reasons.append("Mancanza coordinazione temporale")

        return min(falsification_score, 1.0), reasons

    def classify_phase(self, layer2_output, tie_score, specific_targets):
        """
        3. STATE MACHINE (Il Cervello)
        """
        # Assicuriamoci che specific_targets sia una lista per i check successivi
        if not isinstance(specific_targets, list):
            specific_targets = []

        # Analisi Target (O vs C)
        is_coercive = any(
            t in ['civilian_grid', 'gov_building', 'food_storage'] for t in specific_targets)
        is_offensive = any(
            t in ['ammo_depot', 'fuel', 'bridge', 'command'] for t in specific_targets)

        predicted_phase = "UNCERTAIN"

        # Logica decisionale basata su Layer 2 (Z-Score/Drift) + Target
        if layer2_output == 'ROUTINE':
            predicted_phase = "ATTRITION"

        elif layer2_output == 'SPIKE' or layer2_output == 'DRIFT':
            if is_offensive and not is_coercive:
                predicted_phase = "SHAPING_OFFENSIVE"
            elif is_coercive:
                predicted_phase = "SHAPING_COERCIVE"
            elif is_offensive and is_coercive:
                predicted_phase = "HYBRID_PRESSURE"
            else:
                predicted_phase = "AMBIGUOUS_POSTURE"

        # --- APPLICAZIONE FALSIFICAZIONE ---
        # Passiamo i target come contesto per la falsificazione
        falsify_factor, falsify_reasons = self.falsify_hypothesis(
            tie_score, specific_targets)

        if falsify_factor > 0.6:
            predicted_phase = "INCOHERENT_DISARRAY"

        # --- CALCOLO CONFIDENZA ---
        base_confidence = self._get_historical_confidence(predicted_phase)
        final_confidence = base_confidence * (1.0 - falsify_factor)

        return {
            "phase": predicted_phase,
            "confidence": round(final_confidence, 2),
            "falsification_notes": falsify_reasons,
            "verification_status": "PENDING"
        }

    def detect_signals(self, focus_metric, tempo_metric, depth_metric):
        """
        Pre-processore del Layer 3.
        Trasforma i numeri in 'Segnali Tattici' comprensibili alla State Machine.
        """
        signals = {}

        # 1. PRIORITY INVERSION (Il segnale più pericoloso)
        # Quando l'urgenza (Tempo) è altissima, ma la profondità (Depth/Risorse) è bassa.
        # Indica disperazione o attacco "All-in" improvvisato.
        if tempo_metric > 0.8 and depth_metric < 0.4:
            signals['priority_inversion'] = True
        else:
            signals['priority_inversion'] = False

        # 2. COORDINATED SURGE
        # Quando Focus (argomenti), Tempo (frequenza) e Depth (dettaglio) salgono insieme.
        if focus_metric > 0.7 and tempo_metric > 0.7 and depth_metric > 0.7:
            signals['temporal_coordination'] = True
        else:
            signals['temporal_coordination'] = False

        return signals

    def save_prediction(self, result, tie_score, signals_context):
        """Salva nel Ledger per future verifiche"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = """
        INSERT INTO prediction_history 
        (predicted_phase, confidence_score, tie_score_snapshot, signals_context, verification_status)
        VALUES (?, ?, ?, ?, ?)
        """
        cursor.execute(query, (
            result['phase'],
            result['confidence'],
            tie_score,
            json.dumps(signals_context),
            'PENDING'
        ))
        conn.commit()
        conn.close()
        print(
            f"✅ [MEMORY] Previsione salvata: {result['phase']} (Conf: {result['confidence']})")


def run_post_mortem(self, days_lag=14):
    """
    IL MAESTRO: Chiude il cerchio di apprendimento.
    Controlla le previsioni vecchie e chiede feedback (o lo deduce).
    """
    conn = sqlite3.connect(self.db_path)
    cursor = conn.cursor()

    # 1. Recupera previsioni "PENDING" più vecchie di X giorni
    # (Simuliamo la logica temporale SQL per compatibilità)
    query = f"""
        SELECT id, predicted_phase, created_at, signals_context 
        FROM prediction_history 
        WHERE verification_status = 'PENDING' 
        AND created_at <= date('now', '-{days_lag} days')
        """
    cursor.execute(query)
    pending_predictions = cursor.fetchall()

    if not pending_predictions:
        print("Nessuna previsione passata da verificare.")
        conn.close()
        return

    print(
        f"--- POST MORTEM: Trovate {len(pending_predictions)} previsioni da verificare ---")

    for row in pending_predictions:
        pred_id, phase, date_str, signals = row

        # QUI c'è l'interazione umana o automatica.
        # In un sistema reale full-auto, confronteresti con news attuali.
        # In questo stadio, l'analista deve confermare.
        print(
            f"\n[ID: {pred_id}] In data {date_str} il sistema ha predetto: {phase}")
        print(f"Context: {signals}")

        user_input = input(
            "Questa previsione si è rivelata corretta? (s/n/skip): ").lower()

        new_status = 'PENDING'
        if user_input == 's':
            new_status = 'CONFIRMED'
        elif user_input == 'n':
            new_status = 'FALSE_POSITIVE'

        if new_status != 'PENDING':
            update_query = """
                UPDATE prediction_history 
                SET verification_status = ?, verified_at = CURRENT_TIMESTAMP 
                WHERE id = ?
                """
            cursor.execute(update_query, (new_status, pred_id))
            print(f"✅ Memoria aggiornata: {new_status}")

    conn.commit()
    conn.close()
