from datetime import datetime
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim

class Neural_Network_Cost_Model:
    def __init__(self, input_size, metric='time', hidden_layers=[64,128,128,32], learning_rate=0.001, epochs=50, batch_size=64,dropout=0.1):
        """
        Initialize the neural network model.

        Parameters:
            input_size (int): Number of input features.
            hidden_layers (list): List of neurons in hidden layers.
            learning_rate: (float) Learning rate for optimization.
            epochs: (int) Number of epochs to learn.
            batch_size: (int) SIze of training batches.
            dropout: (float) Value for dropout layers.
        """

        self.input_size = input_size
        self.metric = metric
        self.learning_rate = learning_rate

        self.epochs = epochs
        self.batch_size = batch_size
        self.dropout = dropout


        # Define the neural network architecture
        layers = []
        prev_layer_size = input_size

        '''
        for hidden_size in hidden_layers:
            layers.append(nn.Linear(prev_layer_size, hidden_size))
            layers.append(nn.ReLU())
            prev_layer_size = hidden_size
        '''

        for hidden_size in hidden_layers:
            layers.append(nn.Linear(prev_layer_size, hidden_size))

            # To help with loss fluctuation
            #layers.append(nn.BatchNorm1d(hidden_size))  # Batch normalization
            layers.append(nn.LeakyReLU())  # Leaky ReLU activation
            layers.append(nn.Dropout(self.dropout))  # Dropout for regularization

            prev_layer_size = hidden_size

        layers.append(nn.Linear(prev_layer_size, 1))  # Output layer
        self.model = nn.Sequential(*layers)

        # Define loss function and optimizer
        self.loss_fn = nn.MSELoss()
        self.optimizer = optim.Adam(self.model.parameters(), lr=self.learning_rate)


    def train(self, x_train, y_train):#, epochs=200, batch_size=32):
        """
        Train the neural network model.

         Parameters:
            x_train (dataframe): Input data
            y_train (dataframe): Target data
            epochs (int): Number of training epochs
            batch_size (int): Size of training batches
        """

        epochs = self.epochs
        batch_size = self.batch_size


        x_train = self.extract_relevat_params(x_train)
        y_train = y_train[self.metric]

        latest_loss = 0

        # Convert to torch tensors
        x_train = torch.tensor(x_train.values, dtype=torch.float32)
        y_train = torch.tensor(y_train.values, dtype=torch.float32).view(-1, 1)

        dataset = torch.utils.data.TensorDataset(x_train, y_train)
        dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

        print(f"[{datetime.today().strftime('%H:%M:%S')}] Now starting to train neural network for {epochs} epochs")

        self.model.train()
        for epoch in range(epochs):
            for batch_x, batch_y in dataloader:
                self.optimizer.zero_grad()
                predictions = self.model(batch_x)
                loss = self.loss_fn(predictions, batch_y)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
                self.optimizer.step()

            if (epoch + 1) % 5 == 0:
                print(f"[{datetime.today().strftime('%H:%M:%S')}] Epoch {epoch + 1}/{epochs}, Loss: {loss.item():.4f}")
            latest_loss = loss.item()

        return latest_loss


    def predict(self, x):
        """
        Predict the output for given input data.

        Parameters:
            x (dict): Input config data
        Returns:
            Predicted result metric
        """

        x = self.extract_relevat_params(x)

        # set model to eval mode so to not train on this data point
        self.model.eval()
        x = torch.tensor(x.values, dtype=torch.float32)
        with torch.no_grad():
            prediction = self.model(x).numpy()
        return {self.metric: prediction}


    def update(self, x_new, y_new):
        """
        Update the model with a new data point.
        Uses a higher learning rate to add more weight to the new point

        Parameters:
            x_new (dict): New input config
            y_new (dict): New result value
        """

        x_new = self.extract_relevat_params(x_new)
        y_new = y_new[self.metric]

        # set model into training mode
        self.model.train()
        x_new = torch.tensor(x_new.values, dtype=torch.float32).view(1, -1)
        y_new = torch.tensor(y_new, dtype=torch.float32).view(1, 1)

        #self.optimizer.zero_grad()
        higher_lr = self.learning_rate*2
        update_optimizer = optim.Adam(self.model.parameters(), lr=higher_lr)

        prediction = self.model(x_new)
        loss = self.loss_fn(prediction, y_new)
        loss.backward()

        #self.optimizer.step()
        update_optimizer.step()

        print(f"[{datetime.today().strftime('%H:%M:%S')}] Model updated with new data point. Loss: {loss.item():.4f}")


    def extract_relevat_params(self,data):

        columns = [#"xdbc_version",
            #"run",
            #"format",
            #"client_readmode",
            "client_cpu",
            "server_cpu",
            "network",
            #"network_latency",
            #"network_loss",
            #"source_system",
            #"target_system",
            #"table",
            "bufpool_size",
            "buffer_size",
            #"compression",
            "send_par",
            "rcv_par",
            "write_par",
            "decomp_par",
            "read_partitions",
            "read_par",
            "deser_par",
            "ser_par",
            "comp_par"]

        if isinstance(data, dict):
            data['rcv_par'] = data['send_par']
            data = pd.DataFrame([data])

        if isinstance(data, pd.Series):
            data = dict(data)
            data['rcv_par'] = data['send_par']
            data = pd.DataFrame([data])

        #if all(col in data.columns for col in columns):
        return data[columns]