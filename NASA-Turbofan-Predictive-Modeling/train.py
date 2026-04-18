# train.py
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from tensorflow.keras.models import load_model
import os
from src import config
from src.data_utils import CMAPSSData
from src.model import create_lstm_model

def main():
    dataset_label = 'GOLD_MINIO'
    print(f"--- Starting Pipeline for {dataset_label} ---")
    
    # 1. Prepare Data
    loader = CMAPSSData()
    X_train, y_train = loader.process_data()
    
    model_path = f'model_{dataset_label}.keras'
    
    # 2. Load or Train Model
    if os.path.exists(model_path):
        print(f"Found saved model: {model_path}. Loading...")
        model = load_model(model_path)
    else:
        print(f"No saved model found. Training new model...")
        input_shape = (X_train.shape[1], X_train.shape[2])
        model = create_lstm_model(input_shape)
        
        callbacks = [
            EarlyStopping(monitor='loss', patience=5, restore_best_weights=True),
            ModelCheckpoint(model_path, save_best_only=True, monitor='loss')
        ]
        
        model.fit(
            X_train, y_train,
            epochs=config.EPOCHS,
            batch_size=config.BATCH_SIZE,
            callbacks=callbacks,
            verbose=1
        )

    print("\nTraining completed. Model ready for future test/inference stage.")

if __name__ == "__main__":
    main()