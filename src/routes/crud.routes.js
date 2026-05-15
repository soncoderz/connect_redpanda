const { Router } = require("express");
const createCrudController = require("../controllers/crud.controller");

const createCrudRouter = (defaultTopic) => {
  const router = Router();
  const controller = createCrudController(defaultTopic);

  router.post("/", controller.create);
  router.put("/:id", controller.update);
  router.delete("/:id", controller.remove);
  router.patch("/:id", controller.upsert);

  return router;
};

module.exports = createCrudRouter;
