const { Router } = require("express");
const {
  confirmUser,
  registerUser,
  updateUser,
  deleteUser,
} = require("../controllers/users.controller");

const router = Router();
const asyncHandler = (handler) => (req, res, next) => {
  Promise.resolve(handler(req, res, next)).catch(next);
};

router.post("/", asyncHandler(registerUser));
router.post("/register", asyncHandler(registerUser));
router.get("/confirm", asyncHandler(confirmUser));
router.put("/:id", asyncHandler(updateUser));       
router.delete("/:id", asyncHandler(deleteUser));    

module.exports = router;
