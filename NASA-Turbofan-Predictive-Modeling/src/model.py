# src/model.py
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout

def create_lstm_model(input_shape):
    model = Sequential()
    
    # Layer 1
    model.add(LSTM(units=100, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    
    # Layer 2
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    
    # Output
    model.add(Dense(units=1, activation='linear'))
    
    model.compile(loss='mean_squared_error', optimizer='adam', metrics=['root_mean_squared_error'])
    return model