import { LOGIN_SUCCESS, LOGOUT_SUCCESS } from '../actions/authActions';

const initialState = {
  loggedIn: false,
};

const authReducer = (state = initialState, action) => {
  switch (action.type) {
    case LOGIN_SUCCESS:
      return {
        ...state,
        loggedIn: true,
      };
    case LOGOUT_SUCCESS:
      return {
        ...state,
        loggedIn: false,
      };
    default:
      return state;
  }
};

export default authReducer;
