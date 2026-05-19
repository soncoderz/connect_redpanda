const { Router } = require("express");
const {
  confirmUser,
  registerUser,
  sendFiveConfirmationEmails,
} = require("../controllers/users.controller");

const router = Router();
const asyncHandler = (handler) => (req, res, next) => {
  Promise.resolve(handler(req, res, next)).catch(next);
};

router.post("/", asyncHandler(registerUser));
router.post("/register", asyncHandler(registerUser));
router.get("/confirm", asyncHandler(confirmUser));
// router.post("/send-five-mails", asyncHandler(sendFiveConfirmationEmails));

module.exports = router;
