# Asignaremos el xG de un remate que realizó un jugador X, como Expected Assists para aquel jugador Y que le dió el pase al jugador X

# Necesitamos encontrar las columnas que nos ayuden a referenciar si un record fue un pase que precede a un remate, o si un record
# es directamente un remate

# Filtramos el DataFrame para quedarnos solo con registros que sean pases que precedan a un gol -> con valores en pass_shot_assist
# o disparos con un valor de xG (shot_statsbomb_xg no es nulo).
assist_shot = game_df[(game_df.pass_shot_assist.isnull() == False) | (game_df.shot_statsbomb_xg.isnull() == False)]

assist_shot = assist_shot.sort_values(by=['period', 'timestamp'])
assist_shot = assist_shot.reset_index(drop=True) 
    
    
# Ahora queremos trasladar el valor de xG del disparo al pase previo como su Expected Assist (xA).
# Para ello, usaremos .shift(-1) para mover el xG de un disparo a la fila anterior donde está el pase.
# Esto es clave porque un pase que genera un disparo debería "heredar" el xG de ese disparo.
# - `.loc[]` se usa para seleccionar filas que cumplen una condición específica. En este caso, buscamos donde 'type' sea 'Pass'.
assist_shot.loc[assist_shot['type'] == 'Pass', 'expected_assists_xA'] = assist_shot['shot_statsbomb_xg'].shift(-1)

# Nos quedamos únicamente con las columnas esenciales: 
# - 'id': identificador de la jugada.
# - 'pass_assisted_shot_id': identificador del pase que asistió el disparo.
# - 'expected_assists_xA': el nuevo valor calculado de xA.
assist_shot = assist_shot[['id', 'pass_assisted_shot_id', 'expected_assists_xA']]

# Eliminamos filas con valores nulos para evitar registros incorrectos en el cálculo final.
assist_shot = assist_shot.dropna()

# Finalmente, hacemos un merge con el DataFrame original (game_df) para incorporar los valores de xA.
# Usamos un 'left join' en 'id' y 'pass_assisted_shot_id' para mantener la estructura del DataFrame original.
game_df_xA = pd.merge(game_df, assist_shot, how='left', on=['id', 'pass_assisted_shot_id'])