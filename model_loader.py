import layoutparser as lp

class ModelLoader:
    _instances = {}

    def __new__(cls, model_name):
        if model_name not in cls._instances:
            cls._instances[model_name] = super(ModelLoader, cls).__new__(cls)
            # Load the machine learning model here based on model_name
            model_config = cls.get_model_config(model_name)
            cls._instances[model_name]._model = lp.Detectron2LayoutModel(
                model_config['config'],
                extra_config=model_config['extra_config'],
                label_map=model_config['label_map'],
                device=model_config['device']
            )
        return cls._instances[model_name]
    
    @staticmethod
    def get_model_config(model_name):
        # Define model configurations based on model_name
        model_zoo = {
            'PubLayNet': {
                'config': 'lp://PubLayNet/mask_rcnn_X_101_32x8d_FPN_3x/config',
                'extra_config': ["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                'label_map': {0: "Text", 1: "Title", 2: "List", 3: "Table", 4: "Figure"},
                'device':'cuda'
            },
            'TableBank': {
                'config': 'lp://TableBank/faster_rcnn_R_50_FPN_3x/config',
                'extra_config': ["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                'label_map': {0: "Table"},
                'device':'cuda'
            },
            'MathFormulaDetection': {
                'config': 'lp://MFD/faster_rcnn_R_50_FPN_3x/config',
                'extra_config': ["MODEL.ROI_HEADS.SCORE_THRESH_TEST", 0.8],
                'label_map': {1: "Equation"},
                'device':'cuda'
            }
        }
        
        return model_zoo.get(model_name, {})  # Get model configuration or an empty dictionary

    @property
    def model(self):
        return self._model
    
# if __name__ == "__main__":

#     # Test the singleton with different model names
#     instance1 = ModelLoader("PubLayNet")
#     instance2 = ModelLoader("TableBank")
#     instance3 = ModelLoader("PubLayNet")
#     instance4 = ModelLoader("MathFormulaDetection")  # Reuse the existing model1 instance

#     # Instances with the same model name should be the same
#     print(instance1 is instance3)

#     # Access the loaded model
#     loaded_model1 = instance1.model
#     loaded_model2 = instance2.model
#     loaded_model4 = instance4.model
